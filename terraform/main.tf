terraform {
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
  }
}

provider "azurerm" {
  features {}
}

# Azure Container Registry
resource "azurerm_container_registry" "acr" {
  name                = "eventdriventaskqueue"
  resource_group_name = var.resource_group_name
  location            = var.location
  sku                 = "Basic"
  admin_enabled       = true
}

# Azure Cache for Redis
resource "azurerm_redis_cache" "redis" {
  name                = "event-driven-task-queue-redis"
  location            = var.location
  resource_group_name = var.resource_group_name
  capacity            = var.redis_capacity
  family              = var.redis_family
  sku_name            = var.redis_sku
  enable_non_ssl_port = false
  minimum_tls_version = "1.2"
}

# Postgres Flexible Server
resource "azurerm_postgresql_flexible_server" "postgres" {
  name                   = "event-driven-task-queue-postgres"
  resource_group_name    = var.resource_group_name
  location               = var.location
  version                = "16"
  administrator_login    = var.postgres_admin_user
  administrator_password = var.postgres_admin_password
  sku_name               = "B_Standard_B1ms"
  storage_mb             = 32768
  backup_retention_days  = 7

  authentication {
    active_directory_auth_enabled = false
    password_auth_enabled         = true
  }
}

# Postgres Database
resource "azurerm_postgresql_flexible_server_database" "db" {
  name      = "job_queue"
  server_id = azurerm_postgresql_flexible_server.postgres.id
  collation = "en_US.utf8"
  charset   = "utf8"
}

# Postgres Firewall Rule - allow Azure services
resource "azurerm_postgresql_flexible_server_firewall_rule" "allow_azure" {
  name             = "allow-azure-services"
  server_id        = azurerm_postgresql_flexible_server.postgres.id
  start_ip_address = "0.0.0.0"
  end_ip_address   = "0.0.0.0"
}

# Container Instance - Monitor
resource "azurerm_container_group" "monitor" {
  name                = "event-driven-task-queue-monitor"
  location            = var.location
  resource_group_name = var.resource_group_name
  os_type             = "Linux"
  restart_policy      = "Always"

  image_registry_credential {
    server   = azurerm_container_registry.acr.login_server
    username = azurerm_container_registry.acr.admin_username
    password = azurerm_container_registry.acr.admin_password
  }

  container {
    name   = "monitor"
    image  = var.container_image
    cpu    = "1"
    memory = "2"

    environment_variables = {
      IS_LOCAL       = "false"
      REDIS_HOST     = azurerm_redis_cache.redis.hostname
      REDIS_PORT     = "6380"
      REDIS_DB       = "0"
      POSTGRES_USER  = var.postgres_admin_user
      POSTGRES_DB    = "job_queue"
      POSTGRES_HOST  = azurerm_postgresql_flexible_server.postgres.fqdn
    }

    secure_environment_variables = {
      REDIS_PASSWORD    = azurerm_redis_cache.redis.primary_access_key
      POSTGRES_PASSWORD = var.postgres_admin_password
    }
  }
}

# Variables
variable "resource_group_name" {
  default = "event-driven-task-queue-rg"
}

variable "location" {
  default = "eastus"
}

variable "postgres_admin_password" {
  sensitive = true
}