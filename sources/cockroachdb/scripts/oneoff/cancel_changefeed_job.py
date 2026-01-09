#!/usr/bin/env python3
"""
Shared module for cancelling CockroachDB changefeed jobs.
Used by cancel_job.sh and test_azure_cdc.sh.
"""

import sys
import os
import time
import pg8000
import ssl


def get_connection(conn_url):
    """Create a connection to CockroachDB."""
    parts = conn_url.replace('postgresql://', '').split('@')
    user, password = parts[0].split(':')
    host_port_db = parts[1].split('/')
    host, port = host_port_db[0].split(':')
    database = host_port_db[1].split('?')[0]
    
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    return pg8000.connect(
        user=user,
        password=password,
        host=host,
        port=int(port),
        database=database,
        ssl_context=ssl_context
    )


def get_job_status(cursor, job_id):
    """Get the current status of a job."""
    cursor.execute(f"""
        SELECT job_id, status, running_status
        FROM [SHOW JOBS] 
        WHERE job_id = {job_id}
    """)
    return cursor.fetchone()


def cancel_job(conn_url, job_id, max_attempts=10, poll_interval=10, verbose=True):
    """
    Cancel a CockroachDB changefeed job and wait for confirmation.
    
    Args:
        conn_url: PostgreSQL connection URL
        job_id: Job ID to cancel
        max_attempts: Maximum number of polling attempts (default: 10)
        poll_interval: Seconds between polling attempts (default: 10)
        verbose: Print progress messages (default: True)
    
    Returns:
        dict with:
            - success: bool
            - final_status: str ('canceled', 'removed', 'failed', etc.)
            - message: str
    """
    conn = get_connection(conn_url)
    cursor = conn.cursor()
    
    # Check job exists
    result = get_job_status(cursor, job_id)
    if not result:
        cursor.close()
        conn.close()
        return {
            'success': True,
            'final_status': 'not_found',
            'message': f'Job {job_id} not found (already removed)'
        }
    
    job_id_val, status, running_status = result
    
    if verbose:
        print(f"Job {job_id}: Current status = {status}")
        if running_status:
            print(f"  Running status: {running_status[:200]}")
    
    # Check if already cancelled/failed
    if status in ['canceled', 'cancelled', 'failed']:
        cursor.close()
        conn.close()
        return {
            'success': True,
            'final_status': status,
            'message': f'Job already {status}'
        }
    
    # Issue cancel command
    if verbose:
        print(f"  Issuing CANCEL command...")
    
    try:
        cursor.execute(f"CANCEL JOB {job_id}")
        conn.commit()
        if verbose:
            print(f"  ✅ CANCEL command issued")
    except Exception as e:
        cursor.close()
        conn.close()
        return {
            'success': False,
            'final_status': 'error',
            'message': f'Failed to cancel: {e}'
        }
    
    # Poll for cancellation completion
    if verbose:
        print(f"  ⏳ Polling for completion (every {poll_interval}s, max {max_attempts} attempts)...")
    
    for attempt in range(1, max_attempts + 1):
        time.sleep(poll_interval)
        
        result = get_job_status(cursor, job_id)
        
        if not result:
            # Job no longer exists - fully cancelled
            if verbose:
                print(f"  ✅ Job removed (fully cancelled)")
            cursor.close()
            conn.close()
            return {
                'success': True,
                'final_status': 'removed',
                'message': 'Job successfully removed'
            }
        
        current_status = result[1]
        
        if verbose:
            print(f"    [{attempt}/{max_attempts}] Status: {current_status}")
        
        if current_status in ['canceled', 'cancelled']:
            if verbose:
                print(f"  ✅ Job successfully cancelled")
            cursor.close()
            conn.close()
            return {
                'success': True,
                'final_status': current_status,
                'message': f'Job successfully cancelled'
            }
        elif current_status == 'failed':
            if verbose:
                print(f"  ✅ Job failed (no longer running)")
            cursor.close()
            conn.close()
            return {
                'success': True,
                'final_status': 'failed',
                'message': 'Job failed (no longer running)'
            }
    
    # Timeout - job still processing
    cursor.close()
    conn.close()
    return {
        'success': False,
        'final_status': current_status if result else 'unknown',
        'message': f'Job did not cancel within {max_attempts * poll_interval} seconds (status: {current_status})'
    }


if __name__ == '__main__':
    # CLI usage
    if len(sys.argv) < 2:
        print("Usage: cancel_changefeed_job.py <job_id>")
        print("")
        print("Requires COCKROACHDB_URL environment variable")
        sys.exit(1)
    
    job_id = int(sys.argv[1])
    conn_url = os.environ.get('COCKROACHDB_URL')
    
    if not conn_url:
        print("❌ COCKROACHDB_URL environment variable not set")
        sys.exit(1)
    
    print(f"🔧 Canceling CockroachDB Job: {job_id}")
    print("")
    
    result = cancel_job(conn_url, job_id)
    
    print("")
    if result['success']:
        print(f"✅ {result['message']}")
        print(f"   Final status: {result['final_status']}")
        sys.exit(0)
    else:
        print(f"⚠️  {result['message']}")
        print(f"   Current status: {result['final_status']}")
        print("")
        print(f"💡 The job may still be in the process of cancelling.")
        print(f"   Check back in a few minutes or run again.")
        sys.exit(1)

