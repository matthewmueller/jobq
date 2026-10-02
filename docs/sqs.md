# SQS setup

The `sqs` backend expects each queue, its dead-letter queue and the app's permissions to exist before `Start`. This Terraform creates them.

```hcl
terraform {
  required_providers {
    aws = {
      source = "hashicorp/aws"
    }
  }
}

variable "queues" {
  description = "Queue names, matching each payload's Queue()"
  type        = set(string)
  default     = ["session-run", "session-interrupt"]
}

variable "max_attempts" {
  description = "Total attempts before a job is dead-lettered"
  type        = number
  default     = 5
}

variable "role" {
  description = "Name of the IAM role the app runs as"
  type        = string
}

resource "aws_sqs_queue" "dlq" {
  for_each = var.queues
  name     = "${each.key}-dlq"
  # Dead-lettered messages keep their original enqueue time, so keep them
  # longer than the source queue's default of 4 days
  message_retention_seconds = 1209600 # 14 days, the maximum
}

resource "aws_sqs_queue" "queue" {
  for_each = var.queues
  name     = each.key
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.dlq[each.key].arn
    maxReceiveCount     = var.max_attempts
  })
}

data "aws_iam_policy_document" "jobq" {
  statement {
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:ChangeMessageVisibility",
      "sqs:GetQueueAttributes",
    ]
    resources = [for queue in aws_sqs_queue.queue : queue.arn]
  }
  # Permanent failures are sent to the dead-letter queue, and Revive moves
  # messages from it back to the queue
  statement {
    actions = [
      "sqs:SendMessage",
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:StartMessageMoveTask",
    ]
    resources = [for dlq in aws_sqs_queue.dlq : dlq.arn]
  }
}

resource "aws_iam_role_policy" "jobq" {
  name   = "jobq-sqs"
  role   = var.role
  policy = data.aws_iam_policy_document.jobq.json
}
```

- **Queue names** must match each payload's `Queue()`. SQS names may only use letters, numbers, hyphens and underscores.
- **`max_attempts`** sets `maxReceiveCount`, the total number of attempts before SQS moves a message to the dead-letter queue. Receives interrupted by a shutdown or crash count too.
- **Dead-letter retention** is 14 days. Messages keep their original enqueue time when they're dead-lettered, so a shorter retention could expire them almost immediately.
- **Permissions:** the dead-letter statement covers both `Permanent` failures, which the app sends there directly, and `Revive`, which starts an SQS move task from the dead-letter queue. For an IAM user instead of a role, use `aws_iam_user_policy` with `user`.

Then point `Dial` at the account's queue URL prefix:

```go
queues, err := sqs.Dial(ctx, log, "https://sqs.us-west-2.amazonaws.com/123456789012")
```
