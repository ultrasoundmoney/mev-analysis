Setting up a pgbouncer gateway.

- Set up a VM. Size unknown, starting with N2D-standard-2.
- Either allow http/https traffic or set up a firewall tag plus rule for pgbouncer's port, default 6543.
- On the machine, install pgbouncer: `apt install pgbouncer`.
- Edit `/etc/pgbouncer/pgbouncer.ini` to list the database connection details.

```ini
[databases]
* = host=34.76.37.33 port=5432 dbname=postgres user=postgres password=T9K3nPFRbjds_BCz4kKG auth_user=postgres
```

- Edit `/etc/pgbouncer/userlist.txt` to mimic the details VMs will try to connect with.

```txt
"postgres" "T9K3nPFRbjds_BCz4kKG"
```
