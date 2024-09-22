terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 5.33.0"
    }

    # docker = {
    #   source  = "kreuzwerker/docker"
    #   version = "3.0.2"
    # }
  }

  backend "s3" {
    bucket         = "tf-states-706849440443-eu-west-1"
    key            = "ml-ai-research/terraform.tfstate"
    region         = "eu-west-1"
    dynamodb_table = "tf_states"
  }
}

provider "aws" {
  region  = var.region
  alias   = "aws"
  profile = "default"
}
