#!/usr/bin/env python3
"""
Simple test script to verify MySQL database connection.
"""

import sys

from dotenv import load_dotenv

from db.mysql_connection import get_mysql_connection

# Load environment variables
load_dotenv()


def test_connection():
    """Test MySQL database connection."""
    print("=" * 60)
    print("Testing MySQL Database Connection")
    print("=" * 60)

    connection = None
    try:
        print("\n1. Attempting to connect to MySQL...")
        connection = get_mysql_connection()
        print("   ✅ Connection successful!")

        print("\n2. Testing database query...")
        cursor = connection.cursor()
        cursor.execute("SELECT VERSION()")
        version = cursor.fetchone()[0]
        print(f"   ✅ MySQL version: {version}")

        print("\n3. Checking database name...")
        cursor.execute("SELECT DATABASE()")
        db_name = cursor.fetchone()[0]
        print(f"   ✅ Current database: {db_name}")

        print("\n4. Checking if tables exist...")
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        if tables:
            print(f"   ✅ Found {len(tables)} table(s):")
            for table in tables:
                print(f"      - {table[0]}")
        else:
            print("   ⚠️  No tables found. Run schema.sql to create tables.")

        cursor.close()

        print("\n" + "=" * 60)
        print("✅ All connection tests passed!")
        print("=" * 60)
        return True

    except Exception as e:
        print(f"\n❌ Connection test failed: {e}")
        print("\nTroubleshooting:")
        print("1. Check that MySQL server is running")
        print("2. Verify .env file exists and has correct credentials")
        print("3. Ensure database exists (run: mysql -u root -p < schema.sql)")
        print("4. Check firewall/network settings")
        return False

    finally:
        if connection:
            connection.close()
            print("\nConnection closed.")


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)
