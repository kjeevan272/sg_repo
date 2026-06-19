// CloudWatch alarms on the three SLOs (freshness, completeness, accuracy).
resource "aws_cloudwatch_metric_alarm" "freshness" {
  alarm_name          = "sg-payments-freshness-stale"
  namespace           = "SGPayments"
  metric_name         = "DaysBehind"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 1
  evaluation_periods  = 1
  period              = 3600
  statistic           = "Maximum"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "completeness" {
  alarm_name          = "sg-payments-completeness-breach"
  namespace           = "SGPayments"
  metric_name         = "CompletenessRatio"
  comparison_operator = "LessThanThreshold"
  threshold           = 0.999
  evaluation_periods  = 1
  period              = 3600
  statistic           = "Minimum"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_cloudwatch_metric_alarm" "quarantine_spike" {
  alarm_name          = "sg-payments-quarantine-spike"
  namespace           = "SGPayments"
  metric_name         = "QuarantinedRows"
  comparison_operator = "GreaterThanThreshold"
  threshold           = 100
  evaluation_periods  = 1
  period              = 3600
  statistic           = "Sum"
  alarm_actions       = [aws_sns_topic.alerts.arn]
}

resource "aws_sns_topic" "alerts" {
  name = "sg-payments-alerts"
}
