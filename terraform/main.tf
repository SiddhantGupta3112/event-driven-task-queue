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
  skip_provider_registration = true
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

  container {
    name   = "redis"
    image  = "redis:7-alpine"
    cpu    = "0.5"
    memory = "0.5"

    ports {
      port     = 6379
      protocol = "TCP"
    }
  }

  container {
    name   = "monitor"
    image  = var.container_image
    cpu    = "1"
    memory = "1.5"

    environment_variables = {
      IS_LOCAL      = "false"
      REDIS_HOST    = "localhost"
      REDIS_PORT    = "6379"
      REDIS_DB      = "0"
      POSTGRES_USER = var.postgres_admin_user
      POSTGRES_DB   = "job_queue"
      POSTGRES_HOST = azurerm_postgresql_flexible_server.postgres.fqdn
    }

    secure_environment_variables = {
      POSTGRES_PASSWORD = var.postgres_admin_password
    }
  }
}