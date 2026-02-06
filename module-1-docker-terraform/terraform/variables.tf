variable "project" {
  description = "Project"
  default     = "data-eng-zoomcamp-486520"
}

variable "region" {
  description = "Region"
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery Dataset Name"
  default     = "mod_1_bq_dataset"
}

variable "gcs_bucket_name" {
  description = "My Storage Bucket Name"
  default     = "data-eng-zoomcamp-486520-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}