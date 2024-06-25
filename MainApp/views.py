import csv
import json
import os
from datetime import datetime, timedelta
from django.conf import settings
from django.http import HttpResponse
from django.shortcuts import render
from django.views import View
from django.core.files.storage import default_storage
from django.core.exceptions import ImproperlyConfigured
from django.utils.decorators import sync_and_async_middleware

class CSVUploadView(View):
    async def get(self, request):
        return render(request, 'MainApp/upload.html')

    async def post(self, request):
        file = request.FILES['file']
        timeframe = int(request.POST['timeframe'])
        file_path = default_storage.save(file.name, file)

        candles = await self.process_csv(file_path)
        converted_candles = await self.convert_timeframe(candles, timeframe)
        
        json_path = await self.save_json(converted_candles)
        
        response = HttpResponse(open(json_path, 'rb').read())
        response['Content-Type'] = 'application/json'
        response['Content-Disposition'] = f'attachment; filename={os.path.basename(json_path)}'
        return response

    async def process_csv(self, file_path):
        candles = []
        full_path = os.path.join(settings.MEDIA_ROOT, file_path)
        with open(full_path, mode='r') as file:
            csv_reader = csv.DictReader(file)
            for row in csv_reader:
                candle = {
                    'id': row['BANKNIFTY'],
                    'open': float(row['OPEN']),
                    'high': float(row['HIGH']),
                    'low': float(row['LOW']),
                    'close': float(row['CLOSE']),
                    'date': datetime.strptime(f"{row['DATE']} {row['TIME']}", '%Y%m%d %H:%M')
                }
                candles.append(candle)
        return candles

    async def convert_timeframe(self, candles, timeframe):
        timeframe_candles = []
        start = candles[0]['date']
        end = start + timedelta(minutes=timeframe)
        chunk = []

        for candle in candles:
            if start <= candle['date'] < end:
                chunk.append(candle)
            else:
                if chunk:
                    timeframe_candles.append(self.aggregate_candles(chunk))
                start = candle['date']
                end = start + timedelta(minutes=timeframe)
                chunk = [candle]
        
        if chunk:
            timeframe_candles.append(self.aggregate_candles(chunk))

        return timeframe_candles

    def aggregate_candles(self, candles):
        return {
            'id': candles[0]['id'],
            'open': candles[0]['open'],
            'high': max(candle['high'] for candle in candles),
            'low': min(candle['low'] for candle in candles),
            'close': candles[-1]['close'],
            'date': candles[0]['date'].strftime('%Y%m%d %H:%M')
        }

    async def save_json(self, candles):
        file_path = os.path.join(settings.MEDIA_ROOT, 'candles.json')
        with open(file_path, 'w') as json_file:
            json.dump(candles, json_file)
        return file_path
