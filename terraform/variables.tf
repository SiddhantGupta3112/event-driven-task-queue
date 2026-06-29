variable "redis_capacity" {
  default = 0
}

variable "redis_family" {
  default = "C"
}

variable "redis_sku" {
  default = "Basic"
}

variable "postgres_admin_user" {
  default = "taskqueue_admin"
}

variable "container_image" {
  description = "Full ACR image path for the monitor container"
}