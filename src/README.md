# postgres_dmp
A small tool which triggers postgresql dump for database based on config.yml as input

Example of config.yml

```yaml
data_path: "/data/"
 
rotation:
  period_days: 7
  retention_days: 14
 
servers:
  - db_hostname: host1.example.com
    db_user: user1
    db_password: password1
    db_port: 5432
  - db_hostname: host2.example.com
    db_user: user2
    db_password: password2
    db_port: 5432
  - db_hostname: host3.example.com
    db_user: user3
    db_password: password3
    db_port: 5432
```
