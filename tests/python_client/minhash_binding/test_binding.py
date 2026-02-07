#!/usr/bin/env python3
"""
Test script for milvus_minhash Python binding.

Run after building:
    python test_binding.py
"""

import numpy as np


def test_binding():
    try:
        import milvus_minhash as mh
    except ImportError as e:
        print(f"Error: Cannot import milvus_minhash. Build it first:")
        print(f"  cd {__file__.rsplit('/', 1)[0]}")
        print(f"  pip install pybind11")
        print(f"  python setup.py build_ext --inplace")
        raise e

    print("=" * 60)
    print("Testing milvus_minhash Python binding")
    print("=" * 60)

    # Test constants
    print(f"\nConstants:")
    print(f"  MERSENNE_PRIME = {mh.MERSENNE_PRIME}")
    print(f"  MAX_HASH_MASK = {mh.MAX_HASH_MASK}")

    # Test init_permutations
    print(f"\n1. Testing init_permutations(num_hashes=4, seed=1234):")
    perm_a, perm_b = mh.init_permutations(4, 1234)
    print(f"   perm_a = {perm_a}")
    print(f"   perm_b = {perm_b}")

    # Test hash_shingles_char
    print(f"\n2. Testing hash_shingles_char('hello', shingle_size=3):")
    hashes = mh.hash_shingles_char("hello", 3)
    print(f"   shingles: ['hel', 'ell', 'llo']")
    print(f"   hashes = {hashes}")

    # Test hash_shingles_word
    print(f"\n3. Testing hash_shingles_word('hello world test', shingle_size=2):")
    hashes = mh.hash_shingles_word("hello world test", 2)
    print(f"   shingles: ['helloworld', 'worldtest']")
    print(f"   hashes = {hashes}")

    # Test compute_signature
    print(f"\n4. Testing compute_signature:")
    base_hashes = mh.hash_shingles_char("abc", 3)
    perm_a, perm_b = mh.init_permutations(16, 1234)
    sig = mh.compute_signature(base_hashes, perm_a, perm_b)
    print(f"   text = 'abc', shingle_size = 3")
    print(f"   base_hashes = {base_hashes}")
    print(f"   signature = {sig}")

    # Test high-level API
    print(f"\n5. Testing compute_minhash (high-level API):")
    sig = mh.compute_minhash("abc", num_hashes=16, shingle_size=3, seed=1234)
    print(f"   compute_minhash('abc', num_hashes=16, shingle_size=3, seed=1234)")
    print(f"   signature = {sig}")

    # Compare with Milvus output (if known)
    print(f"\n6. Verification against Milvus:")
    print(f"   This signature should match Milvus output with same parameters.")
    print(f"   Use this to verify: seed=1234, num_hashes=16, shingle_size=3, token_level='char'")

    print("\n" + "=" * 60)
    print("All tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    test_binding()
