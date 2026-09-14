# Socket Abort and Inactivity Teardown

We need to prevent file descriptor leaks and orphaned stream pipelines when clients disconnect prematurely. We decided to bind an `AbortController` to the incoming HTTP request socket lifecycle and a 30-second inactivity timeout, ensuring file descriptors are closed immediately in a guaranteed `finally` block and the session status is marked as `PAUSED` in PostgreSQL. This eliminates OS file descriptor exhaustion under adverse network conditions.
