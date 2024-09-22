resource "random_string" "random_naming" {
  length  = 6
  upper   = false
  special = false
}


data "aws_caller_identity" "current" {}

module "data_storage" {
  providers = {
    aws = aws
  }
  source         = "./infra/aws/modules/storage"
  app_name       = var.app_name
  region         = var.region
  environment    = var.environment
  prefix         = random_string.random_naming.result
  aws_account_id = data.aws_caller_identity.current.account_id
}


module "data_processing" {
  providers = {
    aws = aws
  }
  source         = "./infra/aws/modules/processing"
  key_name       = "${var.app_name}-ssh-key-${var.environment}"
  app_name       = var.app_name
  region         = var.region
  environment    = var.environment
  prefix         = random_string.random_naming.result
  aws_account_id = data.aws_caller_identity.current.account_id
}
