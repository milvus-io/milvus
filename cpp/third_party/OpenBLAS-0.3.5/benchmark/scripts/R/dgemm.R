#!/usr/bin/Rscript

argv <- commandArgs(trailingOnly = TRUE)

nfrom <- 128
nto <- 2048
nstep <- 128
loops <- 1

if (length(argv) > 0) {
  for (z in 1:length(argv)) {
    if (z == 1) {
      nfrom <- as.numeric(argv[z])
    } else if (z == 2) {
      nto <- as.numeric(argv[z])
    } else if (z == 3) {
      nstep <- as.numeric(argv[z])
    } else if (z == 4) {
      loops <- as.numeric(argv[z])
    }
  }

}

p <- Sys.getenv("OPENBLAS_LOOPS")
if (p != "") {
  loops <- as.numeric(p)
}


cat(sprintf(
  "From %.0f To %.0f Step=%.0f Loops=%.0f\n",
  nfrom,
  nto,
  nstep,
  loops
))
cat(sprintf("      SIZE             Flops                   Time\n"))

n <- nfrom
while (n <= nto) {
  A <- matrix(runif(n * n),
              ncol = n,
              nrow = n,
              byrow = TRUE)
  B <- matrix(runif(n * n),
              ncol = n,
              nrow = n,
              byrow = TRUE)
  C <- 1

  z <- system.time(for (l in 1:loops) {
    C <- A %*% B
    l <- l + 1
  })

  mflops <- (2.0 * n * n * n) * loops / (z[3] * 1.0e6)

  st <- sprintf("%.0fx%.0f :", n, n)
  cat(sprintf("%20s %10.2f MFlops %10.6f sec\n", st, mflops, z[3]))

  n <- n + nstep

}
