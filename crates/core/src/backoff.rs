//! Reconnect backoff.
//!
//! Plain exponential backoff has a failure mode at fleet scale: an exchange
//! drops every socket at once, every client waits the same doubling delay, and
//! they all come back in the same instant — the thundering herd that keeps the
//! outage going. Each delay is therefore drawn uniformly from the lower half
//! of the current ceiling upwards ("equal jitter"), which spreads the retries
//! while keeping a floor, so a client cannot hot-loop on a fast failure.

use std::time::Duration;

/// Doubling-with-jitter delays between reconnect attempts.
#[derive(Debug, Clone)]
pub struct Backoff {
    base: Duration,
    max: Duration,
    attempt: u32,
    rng: Xorshift,
}

impl Backoff {
    pub fn new(base: Duration, max: Duration) -> Self {
        Self::with_seed(base, max, seed_from_clock())
    }

    /// Same thing with the jitter source pinned, so tests are deterministic.
    pub fn with_seed(base: Duration, max: Duration, seed: u64) -> Self {
        Self {
            base,
            max: max.max(base),
            attempt: 0,
            rng: Xorshift::new(seed),
        }
    }

    /// Call after a connection has stayed up long enough to count as healthy,
    /// so the next outage starts from the short delay again.
    pub fn reset(&mut self) {
        self.attempt = 0;
    }

    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// The undithered upper bound for the next delay: `base * 2^attempt`,
    /// clamped to `max`.
    pub fn ceiling(&self) -> Duration {
        let factor = 1u64.checked_shl(self.attempt).unwrap_or(u64::MAX);
        self.base
            .checked_mul(u32::try_from(factor).unwrap_or(u32::MAX))
            .unwrap_or(self.max)
            .min(self.max)
    }

    /// The delay to wait before the next attempt, advancing the sequence.
    pub fn next_delay(&mut self) -> Duration {
        let ceiling = self.ceiling();
        self.attempt = self.attempt.saturating_add(1);

        let half = ceiling / 2;
        let spread = ceiling.saturating_sub(half).as_nanos() as u64;
        if spread == 0 {
            return ceiling;
        }

        half + Duration::from_nanos(self.rng.next() % (spread + 1))
    }
}

/// Xorshift64*, inlined rather than pulled in as a dependency: the only thing
/// riding on it is how reconnects are smeared over a second or two.
#[derive(Debug, Clone)]
struct Xorshift(u64);

impl Xorshift {
    fn new(seed: u64) -> Self {
        Self(if seed == 0 {
            0x9E37_79B9_7F4A_7C15
        } else {
            seed
        })
    }

    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.0 = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }
}

fn seed_from_clock() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0x5DEE_CE66)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backoff() -> Backoff {
        Backoff::with_seed(Duration::from_millis(250), Duration::from_secs(30), 42)
    }

    #[test]
    fn the_ceiling_doubles_then_clamps() {
        let mut backoff = backoff();
        let mut ceilings = Vec::new();

        for _ in 0..10 {
            ceilings.push(backoff.ceiling());
            backoff.next_delay();
        }

        assert_eq!(ceilings[0], Duration::from_millis(250));
        assert_eq!(ceilings[1], Duration::from_millis(500));
        assert_eq!(ceilings[2], Duration::from_secs(1));
        assert_eq!(*ceilings.last().unwrap(), Duration::from_secs(30));
    }

    #[test]
    fn every_delay_sits_between_half_the_ceiling_and_the_ceiling() {
        let mut backoff = backoff();

        for _ in 0..200 {
            let ceiling = backoff.ceiling();
            let delay = backoff.next_delay();

            assert!(delay >= ceiling / 2, "{delay:?} < {:?}", ceiling / 2);
            assert!(delay <= ceiling, "{delay:?} > {ceiling:?}");
        }
    }

    #[test]
    fn two_clients_do_not_retry_in_lockstep() {
        // The whole point of the jitter: same schedule, different instants.
        let mut left = Backoff::with_seed(Duration::from_millis(250), Duration::from_secs(30), 1);
        let mut right = Backoff::with_seed(Duration::from_millis(250), Duration::from_secs(30), 2);

        let lefts: Vec<_> = (0..8).map(|_| left.next_delay()).collect();
        let rights: Vec<_> = (0..8).map(|_| right.next_delay()).collect();

        assert_ne!(lefts, rights);
    }

    #[test]
    fn a_healthy_connection_resets_the_sequence() {
        let mut backoff = backoff();
        for _ in 0..5 {
            backoff.next_delay();
        }
        assert_eq!(backoff.attempt(), 5);

        backoff.reset();

        assert_eq!(backoff.attempt(), 0);
        assert_eq!(backoff.ceiling(), Duration::from_millis(250));
    }

    #[test]
    fn a_max_below_the_base_does_not_invert_the_range() {
        let mut backoff = Backoff::with_seed(Duration::from_secs(5), Duration::from_secs(1), 7);

        assert_eq!(backoff.ceiling(), Duration::from_secs(5));
        assert!(backoff.next_delay() >= Duration::from_millis(2_500));
    }

    #[test]
    fn a_very_long_outage_does_not_overflow_the_shift() {
        let mut backoff = backoff();
        for _ in 0..200 {
            backoff.next_delay();
        }

        assert_eq!(backoff.ceiling(), Duration::from_secs(30));
    }
}
