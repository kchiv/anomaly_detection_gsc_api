import argparse
import sys
from datetime import datetime
from datetime import date
from datetime import timedelta
import time
import numpy as np
import pandas as pd
from googleapiclient import sample_tools
import googleapiclient
from prettytable import PrettyTable
import chart_studio
import chart_studio.plotly as py
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import smtplib
from email.MIMEMultipart import MIMEMultipart
from email.MIMEText import MIMEText
from email.MIMEImage import MIMEImage
import config

chart_studio.tools.set_credentials_file(username=config.plotly_username, api_key=config.plotly_api_key)

# URLs to exclude from API calls
blacklist = ['/online-threats/', '/security_response/', '/support/', '/education/']

# Table headers
standard_table = PrettyTable(['Metric', 'Above/Below StDev', 'URL', 'Mean', 'StDev', '# of StDevs', 'Actual'])
flag_table = PrettyTable(['Metric', 'Above/Below StDev', 'URL', 'Mean', 'StDev', '# of StDevs', 'Actual'])

# Sets how many days to look back from current date. Used in timedelta() methods.
# This should help grab the latest date that has available data in GSC API.
recent_date_delta = 2

# Sets one extra day behind from variable recent_date_delta
# This ensures data series is queried excluding most recent day of data
recent_date_delta_add_one = recent_date_delta + 1

# Sets the full date range for GSC API to be queried
# Essentially determines the start date
full_time_series = 60

plotly_list = ''

# Declare command-line flags.
argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('property_uri', type=str,
                       help=('Site or app URI to query data for (including '
                             'trailing slash).'))


def main(argv):
  service, flags = sample_tools.init(
      argv, 'webmasters', 'v3', __doc__, __file__, parents=[argparser],
      scope='https://www.googleapis.com/auth/webmasters.readonly')
  initial_request(service, flags)
  send_email(flags)


def plot_chart(lp_url, full_data_frame, field_text):
  try:
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(go.Scatter(
      x=full_data_frame['Dates'].tolist(),
      y=full_data_frame['Impressions'].tolist(),
      name='Impressions'
      ),
      secondary_y=False,
    )

    fig.add_trace(go.Scatter(
      x=full_data_frame['Dates'].tolist(),
      y=full_data_frame['Clicks'].tolist(),
      name='Clicks'
      ),
      secondary_y=True,
    )

    fig.update_layout(
      xaxis_title = 'Dates'
    )

    fig.update_yaxes(title_text='Impressions', secondary_y=False)
    fig.update_yaxes(title_text='Clicks', secondary_y=True)

    return lp_url, py.plot(fig, auto_open=False)
  except chart_studio.exceptions.PlotlyRequestError:
    return lp_url, 'Account limit reached. Delete charts in Plotly account.'


def standard_dev_calculation(data_list, lp_url, field_text, single_day, full_data_frame):

  global plotly_list
  mean_calc = np.mean(data_list)
  stdev = np.std(data_list)

  i = 3

  if mean_calc >= 100 or single_day >= 100:
    while i > 0:
      stdev_multiplier = stdev * i
      above_mean = mean_calc + stdev_multiplier
      below_mean = mean_calc - stdev_multiplier

      if single_day >= above_mean:
        if i == 3:
          flag_table.add_row([field_text, 'Above', lp_url, mean_calc, stdev, '+' + str(i), single_day])
          lp_url_str, plotly_url_str = plot_chart(lp_url, full_data_frame, field_text)
          plotly_list += field_text + '\n' + lp_url_str + '\n' + plotly_url_str + '\n\n'
        else:
          standard_table.add_row([field_text, 'Above', lp_url, mean_calc, stdev, '+' + str(i), single_day])
        return

      if single_day <= below_mean:
        if i ==3:
          flag_table.add_row([field_text, 'Below', lp_url, mean_calc, stdev, '-' + str(i), single_day])
          lp_url_str, plotly_url_str = plot_chart(lp_url, full_data_frame, field_text)
          plotly_list += field_text + '\n' + lp_url_str + '\n' + plotly_url_str + '\n\n'
        else:
          standard_table.add_row([field_text, 'Below', lp_url, mean_calc, stdev, '-' + str(i), single_day])
        return

      i = i - 1
  else:
    print 'Data set too small ' + lp_url + ' ' + str(single_day)



def second_request(lp_rows, service, flags):
  today_date = date.today()
  latest_date = today_date - timedelta(days=recent_date_delta)
  latest_date_comp = today_date - timedelta(days=recent_date_delta_add_one)
  start_date = today_date - timedelta(days=full_time_series)


  latest_date = latest_date.strftime('%Y-%m-%d')
  latest_date_comp = latest_date_comp.strftime('%Y-%m-%d')
  start_date = start_date.strftime('%Y-%m-%d')


  for lp_row in lp_rows:
    # Grabs URL from response
    lp_url = lp_row['keys'][1]

    # Checks if url contains 'online-threats', 'support', etc. Essentially we're checking for non-important pages and passing on them.
    exist_in_list = [list_exist for list_exist in blacklist if(list_exist in lp_url)]
    # Converts to boolean
    exist_in_list = bool(exist_in_list)

    if exist_in_list == False:

      single_response = gsc_request(latest_date, latest_date, service, flags, lp_url)

      if 'rows' not in single_response:
        print 'Empty response'
        return

      single_row = single_response['rows'][0]
      single_day_clicks = single_row['clicks']
      single_day_impressions = single_row['impressions']

      response = gsc_request(start_date, latest_date_comp, service, flags, lp_url)

      if 'rows' not in response:
        print 'Empty response'
        return

      rows = response['rows']

      clicks_list = []
      impressions_list = []
      date_list = []

      for row in rows:
        clicks_list.append(row['clicks'])
        impressions_list.append(row['impressions'])
        date_list.append(row['keys'][0])

      # Creates dictionary for usage in dataframe
      full_data_dict = {
        'Dates': date_list,
        'Clicks': clicks_list,
        'Impressions': impressions_list
      }

      # Builds dataframe. Dataframe needed for charts.
      full_data_frame = pd.DataFrame(full_data_dict)
      # Appends latest day of data to dataframe
      full_data_frame.loc[full_data_frame.index.max()+1] = [single_day_clicks, latest_date, single_day_impressions]
      # Sorts dataframe
      full_data_frame.sort_values(['Dates'], inplace=True)

      standard_dev_calculation(clicks_list, lp_url, 'Clicks', single_day_clicks, full_data_frame)
      standard_dev_calculation(impressions_list, lp_url, 'Impressions', single_day_impressions, full_data_frame)

    else:
      print 'Not Important Page ' + lp_url



def initial_request(service, flags):
  latest_date = date.today() - timedelta(days=recent_date_delta)

  latest_date = latest_date.strftime('%Y-%m-%d')

  response = gsc_request(latest_date, latest_date, service, flags)

  if 'rows' not in response:
    print 'Empty response'
    return 
  rows = response['rows']
  second_request(rows, service, flags)


def gsc_request(start_date, end_date, service, flags, lp_url=None):

  if lp_url == None:
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['date', 'page'],
        'rowLimit': 5000
      }
  else:
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['date', 'page'],
        'dimensionFilterGroups': [
          {
            'filters': [
              {
                'dimension': 'page',
                'operator': 'equals',
                'expression': lp_url
              }
            ]
          }
        ],
        'rowLimit': 5000
      }

  try:
    response = execute_request(service, flags.property_uri, request)
  except googleapiclient.errors.HttpError as e:
    print e
    time.sleep(60)
    response = execute_request(service, flags.property_uri, request)
  return response

def send_email(flags):
  # email data
  fromaddr = config.from_email
  toaddr = config.to_email
  msg = MIMEMultipart()
  msg['From'] = fromaddr
  msg['To'] = toaddr
  server = smtplib.SMTP('smtp.gmail.com', 587)
  server.starttls()
  server.login(fromaddr, config.email_password)

  msg['Subject'] = flags.property_uri + " Daily Anomaly Detection"
  today_date = date.today()
  date_queried = today_date - timedelta(days=recent_date_delta)
  body = 'Date Queried: ' + date_queried.strftime('%Y-%m-%d') + '\n\n'
  body += 'The following pages featured substantial changes in performance:\n' 
  body += standard_table.get_string(title='Less Than 3 Standard Deviations') 
  body += '\n\n' + flag_table.get_string(title='More Than 3 Standard Deviations')
  body += '\n\n' + plotly_list


  # send email
  msg.attach(MIMEText(body.encode('utf-8'), 'plain', 'utf-8'))
  text = msg.as_string()
  server.sendmail(fromaddr, toaddr, text)
  server.quit()



def execute_request(service, property_uri, request):
  return service.searchanalytics().query(
      siteUrl=property_uri, body=request).execute()


if __name__ == '__main__':
  main(sys.argv)