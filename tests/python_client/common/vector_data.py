# Licensed to the LF AI & Data foundation under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Controlled synthetic dense vectors for functional tests, not a real embedding model."""

import random
from dataclasses import dataclass
from functools import lru_cache

import numpy as np
from sklearn.datasets import make_blobs


@dataclass(frozen=True)
class VectorDataProfile:
    """Versioned workload parameters shared by corpus, queries and index engines."""

    version: str = "clustered-v1"
    center_seed: int = 19530
    weights: tuple[float, ...] = (0.4, 0.3, 0.2, 0.1)
    cluster_std: tuple[float, ...] = (0.6, 0.8, 1.0, 1.2)
    center_scale: float = 0.8
    dimension_scale: tuple[float, float] = (0.75, 1.25)
    neighbor_correlation: float = 0.2
    token_std: float = 0.35


DEFAULT_VECTOR_PROFILE = VectorDataProfile()


@lru_cache(maxsize=32)
def _centers(dim, profile):
    # Centers are independent of the sample seed: separate query/insert calls
    # sample the same population instead of inventing unrelated clusters.
    return np.random.RandomState(profile.center_seed).normal(
        scale=profile.center_scale, size=(len(profile.weights), dim)
    )


def _random_state(seed):
    return np.random.RandomState(random.getrandbits(32) if seed is None else int(seed) % (2**32))


def _correlate(values, profile):
    values = values * np.linspace(*profile.dimension_scale, values.shape[1])
    if values.shape[1] > 1:
        values = values + profile.neighbor_correlation * np.roll(values, 1, axis=1)
    return values


def _finish(values, normalize):
    if normalize and len(values):
        values = values / np.maximum(np.linalg.norm(values, axis=1, keepdims=True), np.finfo(np.float32).tiny)
    return values.astype(np.float32)


def clustered_vectors(count, dim, *, seed=None, normalize=False, profile=DEFAULT_VECTOR_PROFILE):
    """Sample an unequal Gaussian mixture with correlated, nonconstant dimensions.

    The default profile deliberately has overlapping clusters and bounded dimension
    scales. Explicit seeds replay samples; different seeds retain the same centers.
    Normalization is opt-in because IP/L2 may depend on vector magnitudes.
    """
    if count < 0 or dim < 1:
        raise ValueError("count must be nonnegative and dim must be positive")
    if not profile.weights or any(weight <= 0 for weight in profile.weights) or not np.isclose(sum(profile.weights), 1):
        raise ValueError("cluster weights must be positive and sum to one")
    if len(profile.weights) != len(profile.cluster_std) or any(scale <= 0 for scale in profile.cluster_std):
        raise ValueError("each cluster must have a positive standard deviation")
    if min(profile.dimension_scale) <= 0 or abs(profile.neighbor_correlation) >= 1 or profile.token_std <= 0:
        raise ValueError("dimension scales and token_std must be positive and abs(neighbor_correlation) < 1")
    if not count:
        return np.empty((0, dim), dtype=np.float32)
    rng = _random_state(seed)
    counts = rng.multinomial(count, profile.weights)
    values, _ = make_blobs(
        n_samples=counts.tolist(),
        centers=_centers(dim, profile),
        cluster_std=profile.cluster_std,
        shuffle=True,
        random_state=rng,
    )
    return _finish(_correlate(values, profile), normalize)


def clustered_vector_groups(lengths, dim, *, seed=None, normalize=False, profile=DEFAULT_VECTOR_PROFILE):
    """Generate document-correlated token vectors, retaining variable group lengths."""
    if any(length < 0 for length in lengths):
        raise ValueError("group lengths must be nonnegative")
    rng = _random_state(seed)
    documents = clustered_vectors(len(lengths), dim, seed=rng.randint(2**32), profile=profile)
    total = sum(lengths)
    if not total:
        return [np.empty((0, dim), dtype=np.float32) for _ in lengths]
    noise = _correlate(rng.normal(scale=profile.token_std, size=(total, dim)), profile)
    values = _finish(np.repeat(documents, lengths, axis=0) + noise, normalize)
    return np.split(values, np.cumsum(lengths)[:-1])
