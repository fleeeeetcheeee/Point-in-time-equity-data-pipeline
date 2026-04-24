"""
TierZero — Point-in-time equity data pipeline.

Given a (ticker, date) pair, returns every piece of data (price, fundamentals,
index membership, market cap) as it would have been known at market close on
that date. No future information leaks.
"""
