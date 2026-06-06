#!/usr/bin/env Rscript

args <- commandArgs(trailingOnly = TRUE)
required_pkgs <- if (length(args) > 0) {
  args
} else {
  c("consort", "presize", "kappaSize", "irr")
}

cran_repo <- Sys.getenv("CRAN_REPO", unset = "https://cloud.r-project.org")

is_installed <- function(pkg) {
  requireNamespace(pkg, quietly = TRUE)
}

missing_pkgs <- required_pkgs[!vapply(required_pkgs, is_installed, logical(1))]
if (length(missing_pkgs) > 0) {
  install.packages(missing_pkgs, repos = cran_repo)
}

still_missing <- required_pkgs[!vapply(required_pkgs, is_installed, logical(1))]
if (length(still_missing) > 0) {
  stop(
    paste(
      "Missing required R packages after installation attempt:",
      paste(still_missing, collapse = ", ")
    ),
    call. = FALSE
  )
}

message("R packages available: ", paste(required_pkgs, collapse = ", "))
