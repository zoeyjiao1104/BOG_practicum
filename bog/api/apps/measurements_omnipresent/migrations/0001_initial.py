# Generated by Django 4.0 on 2022-09-14 16:04

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        ('sources', '0001_initial'),
        ('measurements_mobile', '0001_initial'),
    ]

    operations = [
        migrations.CreateModel(
            name='OmnipresentMeasurementEvent',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('datetime', models.DateTimeField(verbose_name='Datetime of measurement event')),
                ('anomaly_score', models.FloatField(null=True)),
                ('latitude', models.DecimalField(decimal_places=6, max_digits=9, verbose_name='Latitude of measurement event')),
                ('longitude', models.DecimalField(decimal_places=6, max_digits=9, verbose_name='Longitude of measurement event')),
                ('source', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='sources.source')),
            ],
        ),
        migrations.CreateModel(
            name='OmnipresentMeasurementEventNeighbor',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('distance', models.DecimalField(decimal_places=10, max_digits=20, verbose_name='Distance from neighbor in radians.')),
                ('mobile_event', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='measurements_mobile.mobilemeasurementevent')),
                ('neighboring_omnipresent_event', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='measurements_omnipresent.omnipresentmeasurementevent')),
            ],
        ),
        migrations.CreateModel(
            name='OmnipresentMeasurement',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('product', models.CharField(choices=[('wl', 'Water Level'), ('at', 'Air Temperature'), ('wt', 'Water Temperature'), ('bt', 'Battery Temperature'), ('ws', 'Wind Speed'), ('wd', 'Wind Direction'), ('cs', 'Current Speed'), ('cd', 'Current Direction'), ('bs', 'Buoy Speed'), ('bd', 'Buoy Direction'), ('wcd', 'Wind Cardinal Direction'), ('wgs', 'Wind Gust Speed'), ('w', 'Wind'), ('t', 'Tide'), ('cb', 'Current Bin'), ('cfd', 'Current Flood Direction'), ('ced', 'Current Ebb Direction'), ('cdpth', 'Current Depth'), ('cvm', 'Current Velocity Major'), ('depth', 'Depth'), ('plon', 'Previous Longitude'), ('plat', 'Previous Latitude'), ('a', 'Acceleration'), ('uct', 'UC Temperature'), ('pd', 'Position Delta'), ('momsn', 'momsn'), ('ice', 'Ice percent'), ('err', 'Err (Celsius)'), ('anom', 'Anom (Celsius)'), ('sst', 'Sst (Celsius)'), ('wp', 'Water Pressure'), ('sl', 'Salinity'), ('zc', 'Zonal Current'), ('mc', 'Meridonal Current'), ('lat', 'Latitude'), ('lon', 'Longitude')], max_length=5, null=True)),
                ('value', models.CharField(max_length=10, null=True)),
                ('type', models.CharField(choices=[('r', 'Raw'), ('q1', 'First Quartile'), ('q2', 'Second Quartile'), ('q3', 'Third Quartile'), ('m', 'Mean'), ('pr', 'Prediction'), ('prhl', 'Prediction Highs Lows'), ('in', 'Interpolation'), ('qf', 'Quartod Flags'), ('sd', 'Standard Deviation'), ('q', 'Quality'), ('prm', 'Prediction Mean'), ('prb', 'Prediction Bores')], max_length=5, null=True)),
                ('quality', models.CharField(choices=[('g', 'Good'), ('na', 'Not evaluated or unknown'), ('s', 'Suspect data'), ('b', 'Bad Data')], max_length=2, null=True)),
                ('confidence', models.FloatField(null=True, verbose_name='Confidence in measurement, 1 for observations, [0,1] for predictions')),
                ('omnipresent_measurement_event', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='measurements_omnipresent.omnipresentmeasurementevent')),
            ],
        ),
        migrations.AddConstraint(
            model_name='omnipresentmeasurementeventneighbor',
            constraint=models.UniqueConstraint(fields=('mobile_event', 'neighboring_omnipresent_event'), name='unique_omnipresent_measurement_event_neighbor'),
        ),
        migrations.AddConstraint(
            model_name='omnipresentmeasurementevent',
            constraint=models.UniqueConstraint(fields=('latitude', 'longitude', 'datetime', 'source'), name='unique_omnipresent_measurement_event'),
        ),
        migrations.AddConstraint(
            model_name='omnipresentmeasurement',
            constraint=models.UniqueConstraint(fields=('product', 'type', 'value', 'quality', 'omnipresent_measurement_event'), name='unique_omnipresent_measurement'),
        ),
    ]
