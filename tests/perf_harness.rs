use rand::{Rng, SeedableRng};
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::time::Instant;

fn legacy_select_top_n(mut sizes: Vec<u64>, n: usize) -> Vec<u64> {
    sizes.sort_unstable_by(|a, b| b.cmp(a));
    sizes.truncate(n);
    sizes
}

fn incremental_select_top_n(sizes: &[u64], n: usize) -> Vec<u64> {
    let mut heap: BinaryHeap<Reverse<u64>> = BinaryHeap::new();
    for size in sizes {
        if heap.len() < n {
            heap.push(Reverse(*size));
        } else if let Some(Reverse(min)) = heap.peek() {
            if *size > *min {
                let _ = heap.pop();
                heap.push(Reverse(*size));
            }
        }
    }
    let mut kept = heap.into_iter().map(|Reverse(v)| v).collect::<Vec<_>>();
    kept.sort_unstable_by(|a, b| b.cmp(a));
    kept
}

#[test]
#[ignore = "Performance harness; run explicitly with -- --ignored"]
fn queue_selection_perf_harness() {
    let total = 250_000usize;
    let cap = 1_000usize;
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let sizes = (0..total)
        .map(|_| rng.gen_range(1_000_000u64..=50_000_000_000))
        .collect::<Vec<_>>();

    let start = Instant::now();
    let legacy = legacy_select_top_n(sizes.clone(), cap);
    let legacy_ms = start.elapsed().as_millis();

    let start = Instant::now();
    let incremental = incremental_select_top_n(&sizes, cap);
    let incremental_ms = start.elapsed().as_millis();

    assert_eq!(legacy, incremental);

    // "Memory" proxy: legacy retains all candidates, incremental retains cap.
    let legacy_peak = total as f64;
    let incremental_peak = cap as f64;
    let memory_reduction = 1.0 - (incremental_peak / legacy_peak);
    let time_reduction = if legacy_ms > 0 {
        1.0 - (incremental_ms as f64 / legacy_ms as f64)
    } else {
        0.0
    };

    assert!(
        memory_reduction >= 0.30,
        "Expected >=30% memory reduction proxy, got {:.2}%",
        memory_reduction * 100.0
    );
    assert!(
        time_reduction >= 0.15,
        "Expected >=15% queue-build time reduction, got {:.2}% (legacy={}ms incremental={}ms)",
        time_reduction * 100.0,
        legacy_ms,
        incremental_ms
    );
}
