// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import "strings"

// RegisteredConfigKind identifies how a declared external configuration key
// must be resolved.
type RegisteredConfigKind int

const (
	RegisteredConfigUnknown RegisteredConfigKind = iota
	RegisteredConfigScalar
	RegisteredConfigGroup
)

// sensitiveKeyPatterns is the last-resort classifier: it decides any key that
// no explicit rule claims, whether or not something declares that key.
//
// That "whether or not" is easy to misread and matters. A declared ParamItem
// with a credential-shaped name is caught by this list even though nothing
// marked it Sensitive, which is why proxy.maxPasswordLength and
// proxy.minPasswordLength carry NonSensitive: true — they are length bounds,
// and without the flag they would read as passwords. Declaring a key is not by
// itself a statement that its value is safe.
//
// The list must not be narrower than the audit tripwire in paramtable. A
// manager whose ParamGroup declares the empty prefix (hook.yaml, whose keys are
// plugin-defined and cannot be enumerated by the core) has nothing else to go
// on at all.
var sensitiveKeyPatterns = []string{
	"password",
	"secret",
	"token",
	"credential",
	"privatekey",
	"accesskey",
	"apikey",
	"authparams",
	"saslusername",
	"licensekey",
	// Topology-bearing names. These are primarily a fail-closed fallback for
	// plugin-defined keys; shipped ParamItems and ParamGroups still carry
	// explicit Sensitive metadata audited in paramtable.
	"address",
	"brokerlist",
	"bucketname",
	"endpoint",
	"rootpath",
	"url",
}

// sensitivePatternReplacer normalizes a key before matching it against
// sensitiveKeyPatterns, so that api_key, api-key and apiKey all collapse to the
// same shape.
var sensitivePatternReplacer = strings.NewReplacer("-", "", "_", "", ".", "", "/", "")

// isStoredKey reports whether some source other than the environment, or a
// runtime overlay, holds a value under exactly this spelling.
//
// This is what vouches for a segmentation nothing declared — see
// resolvedKey.segmented — so it excludes the environment for the same reason
// rememberSpelling and groupMemberIsEnvironmentOnly do: environment variable
// names are not a namespace, they are whatever the pod happens to carry, and a
// name that arrives with dots in it ("env 'a.b=c'") would otherwise vouch for
// its own segmentation. groupMemberIsEnvironmentOnly refuses such a key before
// it reaches any exemption today; agreeing with it here means that stays true
// if the two are ever reached in the other order.
func (m *Manager) isStoredKey(dotted string) bool {
	for _, candidate := range [2]string{dotted, strings.ReplaceAll(dotted, ".", "/")} {
		if value, ok := m.overlays.Get(candidate); ok && value != TombValue {
			return true
		}
		if source, ok := m.keySourceMap.Get(candidate); ok && source != environmentSourceName {
			return true
		}
	}
	return false
}

// groupMemberIsEnvironmentOnly reports whether the only thing standing behind a
// prefix-matching key is a process environment variable that happens to
// collapse into the group's separator-free namespace.
//
// A prefix match alone is not enough to trust a caller-supplied key. EnvSource
// stores every variable in the pod under KeyFormatter(name), which is the same
// namespace ParamGroup members are stored in, so a caller could otherwise reach
// an arbitrary variable by re-spelling it with dots until it matched a
// registered prefix — PROXY_ACCESSLOG_FORMATTERS_DATABASE_URL asked for as
// proxy.accessLog.formatters.DATABASE_URL.
//
// A key counts as configuration when some source other than the environment
// supplied it, under either the group's dotted namespace or the separator-free
// identity: an etcd entry written by /management/config/alter is legitimate,
// and a key that exists nowhere yet is a member waiting to be created. Only the
// environment is ambiguous, and only the environment is refused.
//
// The source has to be checked, not merely the presence of the key. EnvSource
// stores each variable under its RAW name as well as the formatted alias
// (env_source.go), and a raw name is frequently already in canonical form —
// PATH, HOSTNAME, http_proxy. Treating "the dotted spelling exists" as proof of
// configuration would hand every such variable to a group whose prefix they
// happen to match, which for the empty prefix is all of them.
func (m *Manager) groupMemberIsEnvironmentOnly(dotted, lookup string) bool {
	// Every spelling the same key can be stored under. EtcdSource keeps whatever
	// separator the stored key used, hence the slash form.
	candidates := []string{
		dotted,
		strings.ReplaceAll(dotted, ".", "/"),
		lookup,
	}
	// The one identity formatKey refuses to produce. It differs from lookup only
	// for knowhere.*, which formatKey exempts and the EnvSource key formatter
	// does not — see strippedKey. Appending it unconditionally would repeat the
	// lookup above for every other key in the table.
	if stripped := strippedKey(dotted); stripped != lookup {
		candidates = append(candidates, stripped)
	}
	sawEnvironment := false
	for _, candidate := range candidates {
		// Overlays are written only through this package's own setters, never
		// from a config source, so a live one is trusted evidence on its own. A
		// tombstone is not: it records that the key was deleted.
		if value, ok := m.overlays.Get(candidate); ok && value != TombValue {
			return false
		}
		source, ok := m.keySourceMap.Get(candidate)
		if !ok {
			continue
		}
		if source != environmentSourceName {
			return false
		}
		sawEnvironment = true
	}
	return sawEnvironment
}

// IsImmutable checks if a configuration key is marked as immutable.
// Uncached for the same reason as resolveRegisteredKey: the management endpoint
// hands this caller-supplied keys.
func (m *Manager) IsImmutable(key string) bool {
	return m.immutableKeys.Contain(formatKeyUncached(key))
}

// RegisterConfigKey records a declared configuration key. Config sources may
// contain arbitrary values, including every process environment variable, so
// safe projections must distinguish declared Milvus configuration from source
// implementation details.
func (m *Manager) RegisterConfigKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.declaredKeys.Insert(formattedKey, lowerKey(strings.ReplaceAll(key, "/", ".")))
	}
}

// RegisterConfigPrefix records a declared dynamic configuration prefix.
//
// An empty prefix declares every key of this manager to be Milvus
// configuration, which is correct only for a table whose sources are all
// operator-authored — hook.yaml is the one such case. It is accepted rather
// than refused because the refusal could not be made reliable: prefixes and
// sources are registered in either order, so a check here would fire or not
// depending on which came first. What actually contains the hazard is
// groupMemberIsEnvironmentOnly, which refuses a key whose only backing is the
// environment whatever prefix matched it —
// TestEmptyPrefixNeverPublishesTheEnvironment is the guarantee.
func (m *Manager) RegisterConfigPrefix(prefix string) {
	canonicalPrefix := strings.ToLower(prefix)
	m.registeredKeyPrefixes.Insert(canonicalPrefix, formatKeyUncached(canonicalPrefix))
}

// RegisterSensitiveKey marks a declared configuration key as sensitive.
func (m *Manager) RegisterSensitiveKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.sensitiveKeys.Insert(formattedKey)
	}
}

// RegisterNonSensitiveKey exempts a reviewed, declared key from the
// secret-name fallback. Use it only for names such as password length or token
// count whose values carry neither credentials nor protected topology.
func (m *Manager) RegisterNonSensitiveKey(key string) {
	formattedKey := formatKey(key)
	if formattedKey != "" {
		m.nonSensitiveKeys.Insert(formattedKey)
	}
}

// RegisterSensitivePrefix marks every configuration below a dynamic prefix as
// sensitive. The empty prefix is accepted and means every key of this manager,
// which is what Sensitive on a group with no KeyPrefix declares; dropping it as
// a no-op would be the one registration path in this file that fails open.
func (m *Manager) RegisterSensitivePrefix(prefix string) {
	canonicalPrefix := strings.ToLower(prefix)
	m.sensitiveKeyPrefixes.Insert(canonicalPrefix)
	m.sensitiveKeyPrefixesCollapsed.Insert(formatKeyUncached(canonicalPrefix))
}

// RegisterNonSensitiveSuffix exempts one leaf name below a sensitive prefix.
//
// A ParamGroup is marked sensitive when its members are provider- or
// plugin-defined and the core cannot enumerate which of them carry credentials
// or topology. That default can be relaxed only for a reviewed leaf whose value
// is neither, such as a pure enable flag. Everything below the prefix that is
// not exempted here still fails closed.
func (m *Manager) RegisterNonSensitiveSuffix(prefix, suffix string) {
	canonicalPrefix := strings.ToLower(prefix)
	canonicalSuffix := strings.ToLower(suffix)
	if canonicalPrefix == "" || canonicalSuffix == "" {
		return
	}
	m.nonSensitiveSuffixes.Insert(suffixExemption(canonicalPrefix, canonicalSuffix))
}

// sensitiveSuffixExemption is the reviewed leaf of one sensitive namespace.
// Keeping the two parts typed avoids an encoded-string invariant in the policy
// registry.
type sensitiveSuffixExemption struct {
	prefix string
	suffix string
}

func suffixExemption(prefix, suffix string) sensitiveSuffixExemption {
	return sensitiveSuffixExemption{prefix: prefix, suffix: suffix}
}

// leafName returns the last dot-separated segment of a canonical key.
func leafName(canonicalKey string) string {
	if idx := strings.LastIndex(canonicalKey, "."); idx >= 0 {
		return canonicalKey[idx+1:]
	}
	return canonicalKey
}

// resolvedKey is one configuration key seen under both identities it can take.
//
// lookup is the separator-free form every source agrees on, and is what values,
// sensitivity marks and immutability are keyed by. dotted is the form that
// carries the key's structure, and is the only one a prefix can be matched
// against — "kafkaproducercompression" cannot be tested against the prefix
// "kafka.producer.", which is why the two must travel together rather than be
// re-derived from whichever spelling a caller happened to use.
type resolvedKey struct {
	lookup string
	dotted string
	kind   RegisteredConfigKind
	// segmented records that dotted came from a declaration or from a source,
	// rather than from whoever asked. Only one rule in this file widens a
	// verdict — a NonSensitiveSuffixes exemption, which is granted to a leaf
	// name — and a leaf name is a property of the segmentation. So that rule
	// may only be applied to a segmentation nothing outside the process chose.
	segmented bool
}

// rememberSpelling records that one configuration key can be addressed under
// two identities, so a later lookup made under the collapsed one can be
// classified against the namespace it belongs to.
//
// Every source inserts a key twice — once as written, once collapsed — so the
// pairing is observable here without any source having to report it. Only
// ParamGroup members need this: a ParamItem records its own spelling in
// declaredKeys, which is consulted first and always wins.
//
// The environment is excluded on purpose. Its variable names are not a
// namespace, they are whatever the pod happens to carry, and letting one of
// them define the structure of a collapsed identity is exactly the
// impersonation groupMemberIsEnvironmentOnly exists to refuse.
func (m *Manager) rememberSpelling(key, sourceName string) {
	if sourceName == environmentSourceName {
		return
	}
	// "/" is what EtcdSource keeps when a key was stored with slashes; "." is
	// every other source. A key with neither is already the collapsed identity
	// and has nothing to teach.
	if !strings.ContainsAny(key, "./") {
		return
	}
	dotted := lowerKey(strings.ReplaceAll(key, "/", "."))
	collapsed := formatKey(key)
	if collapsed == dotted {
		// knowhere.*, which formatKey exempts from collapsing.
		return
	}
	// Two different keys can collapse to one identity ("a.bc" and "ab.c"), and
	// the order sources are walked in is a Go map iteration order. Letting the
	// last writer decide would make the classification of both of them depend on
	// it. Learn nothing instead, and keep learning nothing: with no spelling
	// recovered, the collapsed identity falls back to the collapsed prefixes,
	// which claim rather than exempt. TestDeclaredKeysDoNotCollide rules this
	// out among declared keys; group members are named by whoever wrote them, so
	// a collision between their spellings must fail closed.
	m.spellingMutex.Lock()
	defer m.spellingMutex.Unlock()
	// Deliberately never unlearned, not even when the key is deleted: a learned
	// spelling only ever endorses a segmentation, and endorsing one for an
	// identity that no longer holds a value costs nothing, while dropping it
	// would mean tracking which of the key's two entries went away.
	//
	// Once a collision is recorded, resolveRegisteredKey refuses to endorse
	// either source-backed segmentation. That prevents one spelling's suffix
	// exemption from publishing the value supplied through the other spelling.
	if m.collidedSpellings.Contain(collapsed) {
		return
	}
	if previous, loaded := m.dottedSpellings.GetOrInsert(collapsed, dotted); loaded && previous != dotted {
		m.collidedSpellings.Insert(collapsed)
		m.dottedSpellings.Remove(collapsed)
	}
}

func (m *Manager) resolveRegisteredKey(key string) resolvedKey {
	dotted := lowerKey(strings.ReplaceAll(key, "/", "."))
	// Format the dotted form, not the caller's spelling: formatKey leaves keys
	// below NotFormatPrefix untouched, so "knowhere.X/y" would otherwise keep
	// its slashes and match nothing. Uncached, because this runs on
	// caller-supplied keys from an endpoint anyone can reach, and formatKey's
	// cache is a process-global map that never evicts.
	formattedKey := formatKeyUncached(dotted)
	// A declared ParamItem remembers its own dotted spelling, so it resolves the
	// same way whether the caller used the declared key, its environment alias,
	// or the separator-free identity the value is stored under.
	if declared, ok := m.declaredKeys.Get(formattedKey); ok {
		return resolvedKey{lookup: formattedKey, dotted: declared, kind: RegisteredConfigScalar, segmented: true}
	}

	// Nobody declared this key, so use the spelling the sources showed us for
	// this identity, whatever spelling the caller reached it by.
	//
	// Unconditionally, and that is the whole point. A key's identity is the
	// collapsed form — it is what values are stored under and what an etcd write
	// addresses — so where the separators fall within it is not the caller's to
	// decide. Believing a caller who supplies dots means believing their
	// segmentation, and every rule below reads the segments: a namespace prefix,
	// and the leaf name a NonSensitiveSuffixes exemption is granted to. So
	// "…providers.myprov.credential_url", a credential, could be asked for as
	// "…providers.myprovcredential.url", which is the same stored entry with its
	// leaf renamed to one the group declared safe, and it came back in the clear.
	//
	// Recovery cannot widen anything, because it only fires when the two
	// spellings collapse to the same identity — which is to say, when they are
	// the same entry. It can only put the segments back where the source had
	// them.
	//
	// When nothing was learned — a member that exists only in etcd, which is
	// what an alter-endpoint write leaves behind, or an identity two keys
	// collide on — the caller's segmentation is all there is, and it is then
	// marked unendorsed rather than trusted. See resolvedKey.segmented: an
	// unendorsed segmentation may still name a namespace, because naming one
	// only ever narrows, but it may not claim a suffix exemption.
	segmented := false
	m.spellingMutex.RLock()
	learned, learnedOK := m.dottedSpellings.Get(formattedKey)
	collided := m.collidedSpellings.Contain(formattedKey)
	m.spellingMutex.RUnlock()
	if learnedOK {
		dotted = learned
		segmented = true
	} else if !collided && m.isStoredKey(dotted) {
		// No pairing learned, but a source stored the key under exactly this
		// spelling, so the segmentation is still not the caller's invention. A
		// collided identity is the exception: both spellings are source-backed,
		// but neither one may lend its segmentation to the shared value. Doing so
		// would let one spelling claim a suffix exemption for the other spelling's
		// value.
		segmented = true
	}

	resolved := resolvedKey{lookup: formattedKey, dotted: dotted, segmented: segmented}
	var environmentOnly, environmentBacked *bool
	lazily := func(cached **bool, compute func() bool) bool {
		if *cached == nil {
			verdict := compute()
			*cached = &verdict
		}
		return **cached
	}
	m.registeredKeyPrefixes.Range(func(prefix, collapsedPrefix string) bool {
		switch {
		case hasNamespacePrefix(dotted, prefix):
			// The key names the namespace explicitly, separators and all. Only
			// the environment can produce such a name without meaning it — see
			// groupMemberIsEnvironmentOnly.
			if lazily(&environmentOnly, func() bool {
				return m.groupMemberIsEnvironmentOnly(dotted, formattedKey)
			}) {
				return true
			}
		case !strings.Contains(dotted, ".") && hasNamespacePrefix(formattedKey, collapsedPrefix):
			// Separators already collapsed, so the namespace can only be matched
			// fuzzily — "tlsclustersprodcapempath" against "tlsclusters". That is
			// how AlterConfigsInEtcd stores a member, and how FileSource and
			// EnvSource store their aliases, so the identity has to be accepted
			// or a key written through the alter endpoint becomes invisible to
			// the very projections this file exists to make truthful.
			//
			// Only for a key that has no dots left. A half-collapsed spelling
			// — "kafkaconsumerssl.key.pem", which matches the collapsed prefix
			// while matching no dotted one — names no namespace anybody
			// declared, and the caller who wrote it is the only reason it looks
			// like it does. Such a key is refused rather than admitted as a
			// member of a namespace it only resembles.
			//
			// Sensitivity does not rest on this: matchSensitivePrefix consults
			// the collapsed prefixes unconditionally, so the same spelling
			// classifies sensitive whether or not it is admitted here. Two
			// independent reasons, which is the point — this predicate is about
			// what may be named, not about what may be read.
			//
			// Fuzzy matching is only safe on sources that hold nothing but
			// Milvus configuration. The environment holds everything in the pod,
			// and a collapsed name cannot be checked against the namespace's
			// structure, so an environment-backed identity is not accepted here.
			if lazily(&environmentBacked, func() bool {
				source, ok := m.keySourceMap.Get(formattedKey)
				return ok && source == environmentSourceName
			}) {
				return true
			}
		default:
			return true
		}
		resolved.kind = RegisteredConfigGroup
		return false
	})
	return resolved
}

// hasNamespacePrefix reports whether key names something strictly below prefix.
// An empty prefix covers every non-empty key, which is what a ParamGroup with
// no KeyPrefix declares.
func hasNamespacePrefix(key, prefix string) bool {
	return strings.HasPrefix(key, prefix) && len(key) > len(prefix)
}

// IsSensitive reports whether key carries a credential or protected topology.
// It accepts any spelling of the key — declared, environment alias, or
// separator-free — because it resolves the key first rather than matching
// prefixes against whatever form the caller passed.
func (m *Manager) IsSensitive(key string) bool {
	resolved := m.resolveRegisteredKey(key)
	return m.isSensitiveResolved(resolved)
}

// isSensitiveResolved decides sensitivity from both identities of one key.
//
// Precedence is explicit-before-inferred, and it matters in both directions: a
// declared NonSensitive ParamItem that happens to sit below a sensitive
// ParamGroup prefix (kafka.producer.message.max.bytes) must stay readable, and
// a declared Sensitive key must stay hidden whatever its name looks like.
func (m *Manager) isSensitiveResolved(resolved resolvedKey) bool {
	if m.sensitiveKeys.Contain(resolved.lookup) {
		return true
	}
	if m.nonSensitiveKeys.Contain(resolved.lookup) {
		return false
	}

	// Lower-cased here rather than upstream: lowerKey deliberately leaves
	// NotFormatPrefix keys alone so knowhere index parameters keep the case
	// their engine needs. Every rule below matches against lower-case literals,
	// so without this "knowhere.apiKey" would classify differently from
	// "knowhere.apikey" — the exact four-spellings-of-one-key problem this file
	// exists to remove, reintroduced in the one namespace that legitimately
	// uses mixed case.
	resolved.dotted = strings.ToLower(resolved.dotted)

	switch m.matchSensitivePrefix(resolved) {
	case prefixSensitive:
		return true
	case prefixExempted:
		// A declared suffix exemption is explicit metadata, so it wins over the
		// name-pattern guess below. This is load-bearing today, not a provision
		// for later: the patterns match anywhere in the key, so a provider whose
		// own name contains one — "…providers.mycredential.url" — would be
		// classified sensitive by the fallback despite ending in a leaf the
		// group declared safe. TestNonSensitiveSuffixExemption pins it.
		return false
	}

	patternKey := sensitivePatternReplacer.Replace(resolved.dotted)
	for _, pattern := range sensitiveKeyPatterns {
		if strings.Contains(patternKey, pattern) {
			return true
		}
	}
	return false
}

// prefixVerdict is what the registered sensitive prefixes say about one key.
type prefixVerdict int

const (
	// prefixNoMatch: no sensitive prefix covers the key.
	prefixNoMatch prefixVerdict = iota
	// prefixSensitive: a sensitive prefix covers it and nothing exempts it.
	prefixSensitive
	// prefixExempted: every sensitive prefix that covers it declared this leaf
	// safe.
	prefixExempted
)

// matchSensitivePrefix classifies a key against the registered sensitive
// prefixes, using the dotted identity whenever there is one.
//
// resolveRegisteredKey recovers the dotted spelling for a collapsed key
// whenever some source has shown the manager both, so the common case —
// "kafkaproducersslkeypem", the alias FileSource inserts beside
// "kafka.producer.ssl.key.pem" — arrives here already carrying its structure,
// and gets the same verdict, exemptions included, as the spelling it is an
// alias of.
//
// What is left is a key whose structure nothing in the process knows: a member
// created through /management/config/alter, under a namespace whose members are
// open-ended by definition. There the group's Sensitive default has to stand,
// and it stands two ways. A name with no separators left in it is matched
// against the collapsed prefixes. A name the caller supplied separators for is
// matched against the dotted prefixes as usual, but arrives unendorsed, so the
// exemption below is withheld: "…zilliz.secret_enable" and
// "…zilliz.secret.enable" are one identity, and letting the caller pick which
// of them to send would let them pick the leaf name the exemption is granted
// to, and with it the verdict.
func (m *Manager) matchSensitivePrefix(resolved resolvedKey) prefixVerdict {
	verdict := prefixNoMatch
	m.sensitiveKeyPrefixes.Range(func(prefix string) bool {
		if !strings.HasPrefix(resolved.dotted, prefix) {
			return true
		}
		if resolved.segmented && m.suffixExempted(prefix, resolved.dotted[len(prefix):]) {
			// Exempted for this prefix, but a longer sensitive prefix may still
			// cover the key without exempting it, so keep scanning.
			verdict = prefixExempted
			return true
		}
		verdict = prefixSensitive
		return false
	})
	if verdict != prefixNoMatch {
		return verdict
	}

	// Nothing claimed the dotted identity. Try the collapsed one unconditionally
	// rather than only when the key looks structureless: this is the last thing
	// standing between a namespace declared sensitive and a spelling nobody
	// anticipated, so it errs towards claiming too much. Over-claiming here
	// costs an operator a value they can read under the key's real spelling;
	// under-claiming costs them a private key.
	m.sensitiveKeyPrefixesCollapsed.Range(func(prefix string) bool {
		if strings.HasPrefix(resolved.lookup, prefix) {
			verdict = prefixSensitive
			return false
		}
		return true
	})
	return verdict
}

// suffixExempted reports whether the part of a key below a sensitive prefix is
// one of the leaves the group declared safe. Callers must have established that
// the segmentation is endorsed — see resolvedKey.segmented — because this is
// the one rule in this file that turns a sensitive verdict into a readable one.
//
// The exemption is deliberately shallow: it covers "<provider>.enable" and the
// group's own "enable", but not "<provider>.secret.enable". A group is marked
// sensitive precisely because its member names are open-ended, so an exemption
// that matched the last segment at any depth would let an arbitrary subtree out
// by ending in a safe-looking word.
func (m *Manager) suffixExempted(prefix, remainder string) bool {
	if strings.Count(remainder, ".") > 1 {
		return false
	}
	return m.nonSensitiveSuffixes.Contain(suffixExemption(prefix, leafName(remainder)))
}
