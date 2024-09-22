variable "app_name" {
  type    = string
  default = "cultivo"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "region" {
  type        = string
  default     = "eu-west-1"
  description = "AWS primary region for application"
}

variable "ipv4_cidr" {
  type        = string
  default     = "10.4.0.0/16"
  description = "CIDR block for VPC"
}
