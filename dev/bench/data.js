window.BENCHMARK_DATA = {
  "lastUpdate": 1789124729238,
  "repoUrl": "https://github.com/dasantos/ferx",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "danielsantos5589@gmail.com",
            "name": "Daniel Santos",
            "username": "dasantos"
          },
          "committer": {
            "email": "danielsantos5589@gmail.com",
            "name": "Daniel Santos",
            "username": "dasantos"
          },
          "distinct": true,
          "id": "535ddeb64f4f124435919d2f568fce17d17aad9e",
          "message": "chore: benches",
          "timestamp": "2026-09-11T12:44:16+02:00",
          "tree_id": "0547f6994d6c82811c1648d302b77250cb93ce6d",
          "url": "https://github.com/dasantos/ferx/commit/535ddeb64f4f124435919d2f568fce17d17aad9e"
        },
        "date": 1789123908446,
        "tool": "cargo",
        "benches": [
          {
            "name": "next_no_receivers",
            "value": 12,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "next_with_a_receiver",
            "value": 19,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "next_after_termination",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "danielsantos5589@gmail.com",
            "name": "Daniel Santos",
            "username": "dasantos"
          },
          "committer": {
            "email": "danielsantos5589@gmail.com",
            "name": "Daniel Santos",
            "username": "dasantos"
          },
          "distinct": true,
          "id": "8e50177af4e9108a2079ca63c620b9ac872be22c",
          "message": "fix: clear criterion cache on ci before bench",
          "timestamp": "2026-09-11T13:04:31+02:00",
          "tree_id": "289901c0ed4235e8b922d826f045473e67d3898b",
          "url": "https://github.com/dasantos/ferx/commit/8e50177af4e9108a2079ca63c620b9ac872be22c"
        },
        "date": 1789124727472,
        "tool": "cargo",
        "benches": [
          {
            "name": "next_no_receivers",
            "value": 13,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "next_with_a_receiver",
            "value": 23,
            "range": "± 0",
            "unit": "ns/iter"
          },
          {
            "name": "next_after_termination",
            "value": 4,
            "range": "± 0",
            "unit": "ns/iter"
          }
        ]
      }
    ]
  }
}