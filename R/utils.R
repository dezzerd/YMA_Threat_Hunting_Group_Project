#' @keywords internal
abort_pkg <- function(message, call = rlang::caller_env()) {
  rlang::abort(message, call = call)
}

#' @keywords internal
check_installed <- function(pkgs, call = rlang::caller_env()) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) > 0) {
    abort_pkg(
      paste0(
        "Missing required packages: ",
        paste(missing, collapse = ", "),
        ". Install them first."
      ),
      call = call
    )
  }
  invisible(TRUE)
}

#' @keywords internal
is_ipv6 <- function(x) {
  # DB-IP represents IPv6 addresses with ':'.
  stringr::str_detect(x, ":")
}

#' @keywords internal
ipv4_to_uint32 <- function(x) {
  # Returns numeric (double) to keep full 32-bit unsigned range.
  # Expects dotted IPv4 addresses as character.
  parts <- stringr::str_split_fixed(x, "\\.", 4)
  if (ncol(parts) != 4) return(rep(NA_real_, length(x)))

  a <- suppressWarnings(as.numeric(parts[, 1]))
  b <- suppressWarnings(as.numeric(parts[, 2]))
  c <- suppressWarnings(as.numeric(parts[, 3]))
  d <- suppressWarnings(as.numeric(parts[, 4]))

  bad <- is.na(a) | is.na(b) | is.na(c) | is.na(d) |
    a < 0 | a > 255 | b < 0 | b > 255 | c < 0 | c > 255 | d < 0 | d > 255

  out <- a * 256^3 + b * 256^2 + c * 256 + d
  out[bad] <- NA_real_
  out
}

#' @keywords internal
ipv4_range_to_cidr <- function(ip_start, ip_start_int, ip_end_int) {
  # If the [start,end] range is exactly a CIDR block, return "a.b.c.d/p".
  # Otherwise return NA. Works for IPv4 only.
  if (length(ip_start) == 0) return(character())

  size <- ip_end_int - ip_start_int + 1
  ok <- is.finite(size) & size >= 1

  # Power-of-two check (safe within 32-bit range, representable exactly in double).
  log2_size <- rep(NA_real_, length(size))
  log2_size[ok] <- log2(size[ok])
  pow2 <- ok & is.finite(log2_size) & abs(log2_size - round(log2_size)) < 1e-9

  prefix <- rep(NA_integer_, length(size))
  prefix[pow2] <- as.integer(32 - round(log2_size[pow2]))

  aligned <- pow2 & ((ip_start_int %% size) == 0)

  out <- rep(NA_character_, length(size))
  out[aligned] <- paste0(ip_start[aligned], "/", prefix[aligned])
  out
}


