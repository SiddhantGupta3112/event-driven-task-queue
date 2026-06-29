output "postgres_hostname" {
  value = azurerm_postgresql_flexible_server.postgres.fqdn
}
