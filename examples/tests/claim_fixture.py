"""Resolve a claim's nonce for historical test fixtures, without production bypasses."""
def claim_nonce(store, token, attempt):
    with store._connect() as conn:
        row=conn.execute('SELECT dispatch_nonce FROM events WHERE token=? AND attempts=?',
                         (token,attempt)).fetchone()
    return row[0] if row and row[0] else '0'*32
