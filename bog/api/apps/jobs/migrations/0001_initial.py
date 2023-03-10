# Generated by Django 4.0 on 2022-09-12 22:55

from django.db import migrations, models
import uuid


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Job',
            fields=[
                ('id', models.UUIDField(default=uuid.uuid4, editable=False, primary_key=True, serialize=False)),
                ('name', models.CharField(choices=[('data-retention', 'Data Retention'), ('load-measurements', 'Load Measurements'), ('refresh-oscar-datasets', 'Load Oscar Datasets'), ('train-anomaly-detection', 'Train Anomaly Detection'), ('train-prediction-models', 'Train Predictions'), ('run-anomaly-detection', 'Run Anomaly Detection'), ('run-prediction-models', 'Run Prediction Models')], max_length=255)),
                ('status', models.CharField(choices=[('completed', 'Completed'), ('error', 'Error'), ('running', 'Running')], max_length=255)),
                ('query_date_start_utc', models.DateTimeField()),
                ('query_date_end_utc', models.DateTimeField()),
                ('started_at_utc', models.DateTimeField(auto_now_add=True)),
                ('completed_at_utc', models.DateTimeField(null=True)),
                ('last_error_at_utc', models.DateTimeField(null=True)),
                ('error_message', models.TextField(null=True)),
                ('retry_count', models.SmallIntegerField(default=0)),
            ],
        ),
    ]
