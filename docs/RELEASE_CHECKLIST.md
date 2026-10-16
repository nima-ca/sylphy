# Release checklist (v0.3.0)

1. `task test-race` (includes `./test/e2e`; also run `task test-e2e` once, since it uses `-count=1`).
2. `task lint`.
3. `task fuzz` (four targets, 20s each). Confirm CI's fuzz job runs the same four.
4. `task bench-persist` on the machine you will quote. Fill the TBD cells in `docs/BENCHMARKS.md` with machine, disk and date.
5. Manual kill-and-recover: follow the walkthrough in `docs/PERSISTENCE.md` (`kill -9` and recovery; `SAVE` and the directory listing; torn tail with `-aof-load-truncated` true and false).
6. `task docker`, then a named-volume restart test:

    ```bash
    docker volume create sylphy-data
    docker run -d --name sylphy -p 6380:6380 -v sylphy-data:/data \
      -e SYLPHY_PERSISTENCE_MODE=aof -e SYLPHY_APPEND_FSYNC=always sylphy:<tag>
    redis-cli -p 6380 SET greeting hello
    docker kill sylphy && docker rm sylphy          # hard kill
    docker run -d --name sylphy -p 6380:6380 -v sylphy-data:/data \
      -e SYLPHY_PERSISTENCE_MODE=aof sylphy:<tag>
    redis-cli -p 6380 GET greeting                  # expect hello
    docker stop sylphy                              # graceful: final checkpoint
    ```

    Start it again and `GET` once more. `<tag>` is the output of `git describe --tags --always --dirty`.

7. Confirm the CHANGELOG date, commit, tag `v0.3.0`, push the tag.
