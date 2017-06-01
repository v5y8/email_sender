#! /usr/local/bin/python2.7
# -*- coding: utf-8 -*-
from __future__ import print_function
__author__ = 'jliu, mjacobson'

from bs4 import BeautifulSoup
import datetime
from dateutil.parser import parse
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import MySQLdb
import _mysql_exceptions

CREDS = {'host': 'localhost',
         'user': 'confluence',
         'passwd': 'ec8euSho',
         'db': 'confluence',
         'port': 3306
         }

# timestamp for debugging/info
timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

def get_connection():
    """Retrieve a connection to the MySQL DB on Kang, requires shh tunnel"""
    try:
        con = MySQLdb.connect(**CREDS)
    except _mysql_exceptions.OperationalError:
        print("Can't connect to DB; try creating ssh tunnel: ssh -f otto -L 9999:kang:3306 -N")
        raise
    except Exception:
        print("Problem with connecting to database")
        raise
    return con


def fetch_html():
    """Retrieve HTML for http://www.chibi.ubc.ca/faculty/pavlidis/wiki/display/PavLab/Group+Meeting+Schedule"""
    con = get_connection()

    with con as cur:
        try:
            cur.execute("SELECT BODY from BODYCONTENT where CONTENTID=182")
            html = cur.fetchone()

            return html[0]

        except Exception as inst:
            print('Error rolling back', inst)
            con.rollback()
        finally:
            if cur:
                cur.close()


def fetch_users():
    """Retrieve a mapping of unique user key to (name, email)"""
    con = get_connection()

    with con as cur:
        try:
            cur.execute("SELECT user_name, display_name,  email_address  FROM cwd_user")
            users = cur.fetchall()
            name_to_email = {user[0]: user[2] for user in users}

            cur.execute(" SELECT user_key, username from user_mapping")
            keys = cur.fetchall()
            key_to_name = {user_key[0]: user_key[1] for user_key in keys}

            key_to_user = {key: (name, name_to_email[name]) for key, name in key_to_name.iteritems()
                            if name in name_to_email}

            return key_to_user

        except Exception as inst:
            print('Error rolling back', inst)
            con.rollback()
        finally:
            if cur:
                cur.close()


def send_emails(table_col, subject, message):
    """Entry Point
       Scrapes HTML of Group Meeting Schedule from confluence MySQL DB
       Emails users who are presenting in the current week"""
    

    # populate the dictionaries for uses, email addresses, and schedule html.
    key_to_user = fetch_users()
    soup = BeautifulSoup(fetch_html(), "html.parser")

    table = soup.find('table')
    rows = table.findAll('tr')
    rows.pop(0)  # remove header row
    relevant_rows = [[extract_date(row), row.findAll("td")] for row in rows if 0 <= date_gap(extract_date(row)) <= 6]

    if len(relevant_rows) > 1:
        raise ValueError(timestamp + ": Multiple rows match current week")
    
    elif len(relevant_rows) == 0:
        raise ValueError(timestamp + ": No rows match current week")

    col_date, cols = relevant_rows[0]

    user_keys = extract_users_keys_from_cell(cols[table_col])  # Presenter column

    if len(user_keys) == 0:
        print("No user found in column {0} for current week {1}".format(table_col, col_date))

    for u_key in user_keys:
        try:
            name, email = key_to_user[u_key]
            write_email(email, subject, message.format(date=col_date))
            print(timestamp + ": Email sent to: " + str((name, email)))

        except smtplib.SMTPException as e:
            print(timestamp + ": Failed to send email to: " + str((name, email)), e)
        
        except KeyError:
            print(timestamp + ": unable to find email address for: " + u_key)


def extract_users_keys_from_cell(cell):
    users = cell.find_all("ri:user")
    if users is None:
        return []

    return [user['ri:userkey'] for user in users]


def write_email(address, subject, body):
    """Given email address and message body, send email."""

    host = 'localhost'
    sender = "Pavlab@chibi.ubc.ca"

    msg = MIMEMultipart('alternative')
    msg['Subject'] = subject
    msg['From'] = sender
    msg['To'] = address

    # attaches input body of email to message
    message = MIMEText(body, "html")
    msg.attach(message)
    try:
        smtp_obj = smtplib.SMTP(host)
        smtp_obj.sendmail(sender, address, msg.as_string())
    except smtplib.SMTPException:
        raise


def extract_date(row):
    return parse(row.findAll("td")[0].find(text=True)).date()


def date_gap(col_date):
    try:

        # now compare the cell's date with the current date
        curr_date = datetime.date.today()

        return (col_date - curr_date).days
    except IndexError:
        return None


def test():
    d = "2015-01-01"
    col_date = parse(d).date()
    presenter_message = '''You are due to present on {date}.
    Check the <a href="http://www.chibi.ubc.ca/faculty/pavlidis/wiki/display/PavLab/Group+Meeting+Schedule">meeting schedule</a> for details.'''.format(date=col_date)

    write_email("JacobsonMT@gmail.com", "You are presenting this week", presenter_message)

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='Sends emails to presenters and treat bringers for group meetings.')

    parser.add_argument('--dry-run', dest='dry', action='store_true',
                       help='Dry run')

    group = parser.add_mutually_exclusive_group()

    group.add_argument('--all', dest='all', action='store_true',
                       help='Sends emails to presenters and treat bringers. DEFAULT')
    group.add_argument('--treat', dest='treat', action='store_true',
                       help='Sends emails to treat bringers only.')
    group.add_argument('--present', dest='present', action='store_true',
                       help='Sends emails to presenters only.')

    args = parser.parse_args()

    # write_email = lambda x, y, z: print('Mock Write', x, y, z)

    presenter_column = 2
    presenter_message = '''You are due to present on {date}.
    Check the <a href="http://www.chibi.ubc.ca/faculty/pavlidis/wiki/display/PavLab/Group+Meeting+Schedule">meeting schedule</a> for details.'''

    treats_column = 3
    treats_message = '''You are due to bring treats on {date}.
    Check the <a href="http://www.chibi.ubc.ca/faculty/pavlidis/wiki/display/PavLab/Group+Meeting+Schedule">meeting schedule</a> for details.'''
    if args.dry:
        print('Dry Run')
        write_email = lambda x, y, z: print('Mock Write', x, y, z)

    if args.all:
        print('All')
        send_emails(presenter_column, "You are presenting this week", presenter_message)
        send_emails(treats_column, "You are bringing treats this week", treats_message)
    elif args.treat:
        print('Treats')
        send_emails(treats_column, "You are bringing treats this week", treats_message)
    elif args.present:
        print('Presenters')
        send_emails(presenter_column, "You are presenting this week", presenter_message)
    else:
        print('All')
        send_emails(presenter_column, "You are presenting this week", presenter_message)
        send_emails(treats_column, "You are bringing treats this week", treats_message)

