output "redis_hostname" {
  value = azurerm_redis_cache.redis.hostname
}

output "redis_primary_key" {
  value     = azurerm_redis_cache.redis.primary_access_key
  sensitive = true
}

output "postgres_hostname" {
  value = azurerm_postgresql_flexible_server.postgres.fqdn
}

output "acr_login_server" {
  value = azurerm_container_registry.acr.login_server
}