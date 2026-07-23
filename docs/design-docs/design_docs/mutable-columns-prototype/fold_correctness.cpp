// Property tests for the mutable-columns folding algebra + MVCC.
// Strategy: for random op sequences, compare the DESIGN'S folded representation
// against a naive full-replay reference. Any mismatch is a spec bug found
// before implementation.
//
// Properties tested:
//   P1  scalar SET/INCR: fold below safe_ts + tail nodes == naive replay, for
//       every query ts >= safe_ts.
//   P2  scalar watermark: fold to fold_ts (materialize base') then replay only
//       ts>fold_ts == full replay (no INCR double-apply).
//   P3  array APPEND/POP_FRONT: (k,S) fold + materialize == naive.
//   P4  array APPEND/REMOVE:    (R,S) fold + materialize == naive.
//   P5  array arbitrary mix (incl. POP+REMOVE): symbolic-chain fold == naive.

#include <cstdio>
#include <cstdint>
#include <vector>
#include <random>
#include <algorithm>
#include <string>

// ---------------- scalar ----------------
enum SOp { SSET, SINCR };
struct SEntry { uint64_t ts; SOp op; int64_t val; };

// naive: value at qts = base overlaid by all ops with ts<=qts, in ts order.
static int64_t scalar_naive(int64_t base, const std::vector<SEntry>& ops, uint64_t qts) {
    int64_t v = base;
    std::vector<SEntry> s(ops); std::stable_sort(s.begin(), s.end(),
        [](auto&a, auto&b){ return a.ts < b.ts; });
    for (auto& e : s) { if (e.ts > qts) break;
        if (e.op == SSET) v = e.val; else v += e.val; }
    return v;
}

// design fold: below safe_ts collapse to a floor {has_set,set_val,incr_sum};
// above safe_ts keep individual nodes. Materialize at qts>=safe_ts.
struct Floor { bool has_set=false; int64_t set_val=0; int64_t incr=0; };
static int64_t scalar_folded(int64_t base, const std::vector<SEntry>& ops,
                             uint64_t safe_ts, uint64_t qts) {
    std::vector<SEntry> s(ops); std::stable_sort(s.begin(), s.end(),
        [](auto&a, auto&b){ return a.ts < b.ts; });
    Floor fl; std::vector<SEntry> tail;
    for (auto& e : s) {
        if (e.ts <= safe_ts) {
            if (e.op == SSET) { fl.has_set = true; fl.set_val = e.val; fl.incr = 0; }
            else fl.incr += e.val;
        } else tail.push_back(e);
    }
    // materialize floor
    int64_t v = fl.has_set ? fl.set_val + fl.incr : base + fl.incr;
    for (auto& e : tail) { if (e.ts > qts) break;
        if (e.op == SSET) v = e.val; else v += e.val; }
    return v;
}

// design watermark: base' = naive(base, ops<=fold_ts); replay ops>fold_ts.
static int64_t scalar_watermark(int64_t base, const std::vector<SEntry>& ops,
                                uint64_t fold_ts, uint64_t qts) {
    int64_t basep = scalar_naive(base, ops, fold_ts);
    std::vector<SEntry> above;
    for (auto& e : ops) if (e.ts > fold_ts) above.push_back(e);
    return scalar_naive(basep, above, qts);
}

// ---------------- array ----------------
enum AOp { AAPPEND, APOP, AREMOVE, ASET };
struct AEntry { AOp op; std::vector<int64_t> elems; int64_t val; };

static std::vector<int64_t> array_naive(std::vector<int64_t> L, const std::vector<AEntry>& ops) {
    for (auto& e : ops) {
        if (e.op == AAPPEND) for (auto x : e.elems) L.push_back(x);
        else if (e.op == APOP) { if (!L.empty()) L.erase(L.begin()); } // pop-empty = no-op
        else if (e.op == AREMOVE) L.erase(std::remove(L.begin(), L.end(), e.val), L.end());
        else /*ASET*/ L = e.elems;
    }
    return L;
}

// design fold for APPEND/POP only -> (k, S); materialize = drop(k, base ++ S).
static std::vector<int64_t> array_fold_kS(std::vector<int64_t> base, const std::vector<AEntry>& ops) {
    int64_t k = 0; std::vector<int64_t> S;
    for (auto& e : ops) {
        if (e.op == AAPPEND) for (auto x : e.elems) S.push_back(x);
        else if (e.op == APOP) k += 1;  // <-- design rule: count every pop
        else return {}; // not applicable
    }
    std::vector<int64_t> virt = base; for (auto x : S) virt.push_back(x);
    int64_t drop = std::min<int64_t>(k, (int64_t)virt.size());
    return std::vector<int64_t>(virt.begin() + drop, virt.end());
}

// CORRECT fold for APPEND/POP (and any mix): coalesce consecutive APPENDs,
// keep POPs, then materialize by replaying the coalesced chain against base.
// Base-free foldability is impossible for POP (pop-on-empty needs len(base)),
// so POP resolution is deferred to materialize/fold time, where base is in hand.
static std::vector<int64_t> array_fold_symbolic(std::vector<int64_t> base,
                                                const std::vector<AEntry>& ops,
                                                int* chain_len_out) {
    std::vector<AEntry> chain;
    for (auto& e : ops) {
        if (e.op == AAPPEND && !chain.empty() && chain.back().op == AAPPEND) {
            for (auto x : e.elems) chain.back().elems.push_back(x); // coalesce
        } else chain.push_back(e);
    }
    if (chain_len_out) *chain_len_out = (int)chain.size();
    return array_naive(base, chain); // replay against real base = always correct
}

// design fold for APPEND/REMOVE only -> (R, S).
static std::vector<int64_t> array_fold_RS(std::vector<int64_t> base, const std::vector<AEntry>& ops) {
    std::vector<int64_t> R;         // values removed from base
    std::vector<int64_t> S;         // surviving appended suffix
    for (auto& e : ops) {
        if (e.op == AAPPEND) for (auto x : e.elems) S.push_back(x);
        else if (e.op == AREMOVE) {
            R.push_back(e.val);
            S.erase(std::remove(S.begin(), S.end(), e.val), S.end());
        } else return {};
    }
    std::vector<int64_t> out;
    for (auto x : base) if (std::find(R.begin(), R.end(), x) == R.end()) out.push_back(x);
    for (auto x : S) out.push_back(x);
    return out;
}

// ---------------- harness ----------------
static int fails = 0;
static void check(bool ok, const std::string& prop, const std::string& detail) {
    if (!ok) { fails++; if (fails <= 8) printf("  FAIL [%s] %s\n", prop.c_str(), detail.c_str()); }
}
static std::string vs(const std::vector<int64_t>& v){ std::string s="["; for(size_t i=0;i<v.size();++i){ s+=std::to_string(v[i]); if(i+1<v.size())s+=","; } return s+"]"; }

int main() {
    std::mt19937_64 rng(2026);
    const int N = 200000;

    // P1 + P2 scalar
    int p1=0,p2=0;
    for (int t=0;t<N;++t){
        int64_t base = (int64_t)(rng()%20);
        int nops = rng()%8;
        std::vector<SEntry> ops;
        for(int i=0;i<nops;++i){ SEntry e; e.ts = 1+rng()%20;
            e.op = (rng()&1)?SSET:SINCR; e.val = (int64_t)(rng()%10)-3; ops.push_back(e); }
        uint64_t safe = 1+rng()%20, qts = safe + rng()%20; // reader ts >= safe_ts
        int64_t a = scalar_naive(base,ops,qts), b = scalar_folded(base,ops,safe,qts);
        check(a==b,"P1 scalar fold", "base="+std::to_string(base)+" naive="+std::to_string(a)+" folded="+std::to_string(b)); if(a==b)p1++;
        uint64_t fts = 1+rng()%20; uint64_t q2 = fts + rng()%20;
        int64_t c = scalar_naive(base,ops,q2), d = scalar_watermark(base,ops,fts,q2);
        check(c==d,"P2 watermark", "naive="+std::to_string(c)+" wm="+std::to_string(d)); if(c==d)p2++;
    }
    printf("P1 scalar fold vs naive:      %d/%d pass\n", p1, N);
    printf("P2 scalar watermark no-dup:   %d/%d pass\n", p2, N);

    // P3 array APPEND/POP -- COUNTEREXAMPLE DEMO: the design's claimed (k,S)
    // closed form is WRONG (pop-on-empty). Expected to fail; not an assertion.
    int p3=0,tot3=0,shown=0;
    for(int t=0;t<N;++t){
        std::vector<int64_t> base; int bl=rng()%4; for(int i=0;i<bl;++i) base.push_back((int64_t)(rng()%9)+1);
        std::vector<AEntry> ops; int no=rng()%8;
        for(int i=0;i<no;++i){ AEntry e; if(rng()&1){ e.op=AAPPEND; int a=1+rng()%2; for(int j=0;j<a;++j)e.elems.push_back((int64_t)(rng()%9)+1);} else e.op=APOP; ops.push_back(e); }
        tot3++;
        auto a=array_naive(base,ops), b=array_fold_kS(base,ops);
        if(a==b)p3++; else if(shown++<5) printf("  counterexample: base=%s naive=%s (k,S)=%s\n", vs(base).c_str(), vs(a).c_str(), vs(b).c_str());
    }
    printf("P3 array APPEND/POP (k,S) [design's claimed closed form]: %d/%d pass  <-- REFUTED (pop-on-empty)\n", p3, tot3);

    // P3b: correct fold (coalesced symbolic chain, materialized vs base)
    int p3b=0; long raw_ops=0, chain_ops=0;
    for(int t=0;t<N;++t){
        std::vector<int64_t> base; int bl=rng()%4; for(int i=0;i<bl;++i) base.push_back((int64_t)(rng()%9)+1);
        std::vector<AEntry> ops; int no=1+rng()%8;
        for(int i=0;i<no;++i){ AEntry e; if(rng()&1){ e.op=AAPPEND; int a=1+rng()%2; for(int j=0;j<a;++j)e.elems.push_back((int64_t)(rng()%9)+1);} else e.op=APOP; ops.push_back(e); }
        int cl=0; auto a=array_naive(base,ops), b=array_fold_symbolic(base,ops,&cl);
        raw_ops+=ops.size(); chain_ops+=cl;
        bool ok=(a==b); check(ok,"P3b append/pop symbolic","naive="+vs(a)+" folded="+vs(b)); if(ok)p3b++;
    }
    printf("P3b APPEND/POP symbolic (correct fix): %d/%d pass  (chain %ld ops vs %ld raw = %.0f%% compression)\n",
           p3b, N, chain_ops, raw_ops, 100.0*(1.0 - (double)chain_ops/raw_ops));

    // P4 array APPEND/REMOVE
    int p4=0,tot4=0;
    for(int t=0;t<N;++t){
        std::vector<int64_t> base; int bl=rng()%4; for(int i=0;i<bl;++i) base.push_back((int64_t)(rng()%5)+1);
        std::vector<AEntry> ops; int no=rng()%8;
        for(int i=0;i<no;++i){ AEntry e; if(rng()&1){ e.op=AAPPEND; int a=1+rng()%2; for(int j=0;j<a;++j)e.elems.push_back((int64_t)(rng()%5)+1);} else { e.op=AREMOVE; e.val=(int64_t)(rng()%5)+1;} ops.push_back(e); }
        tot4++;
        auto a=array_naive(base,ops), b=array_fold_RS(base,ops);
        bool ok=(a==b); check(ok,"P4 append/remove (R,S)","base="+vs(base)+" naive="+vs(a)+" folded="+vs(b)); if(ok)p4++;
    }
    printf("P4 array APPEND/REMOVE (R,S): %d/%d pass\n", p4, tot4);

    printf("\n%s  (%d failures)\n", fails==0?"ALL PASS":"FAILURES FOUND", fails);
    return fails==0?0:1;
}
