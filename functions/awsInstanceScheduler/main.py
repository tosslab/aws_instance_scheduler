import sys
sys.path.append('./python_modules')

import os
import time
import uuid
import traceback
import requests
import json
from datetime import datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Attr

os.environ['TZ'] = 'Asia/Seoul'
time.tzset()

WEBHOOK_URL = os.environ['WEBHOOK_URL']
OUTGOING_WEBHOOK_TOKEN = os.environ['OUTGOING_WEBHOOK_TOKEN']
STOP_ALERT_BEFORE_TIME_MINUTE = int(os.environ['STOP_ALERT_BEFORE_TIME_MINUTE'])


class ScheduleUtil:
    @staticmethod
    def get_ec2_instance_list_by_status(ec2_instance_list, status) -> list:
        filtered_ec2_instance_list = []

        for ec2_instance in ec2_instance_list:
            if ScheduleUtil.get_ec2_instance_status(ec2_instance) == status:
                filtered_ec2_instance_list.append(ec2_instance)

        return filtered_ec2_instance_list

    @staticmethod
    def get_ec2_instance_ids(ec2_instance_list) -> list:
        ec2_instance_ids = []

        for ec2_instance in ec2_instance_list:
            ec2_instance_ids.append(ec2_instance['InstanceId'])

        return ec2_instance_ids

    @staticmethod
    def get_ec2_instance_status(ec2_instance) -> str:
        return ec2_instance['State']['Name']

    @staticmethod
    def get_ec2_instance_name(ec2_instance, default_name='EC2') -> str:
        tag_list = ec2_instance['Tags']

        for tag in tag_list:
            if tag['Key'] == 'Name':
                return tag['Value']

        return default_name

    @staticmethod
    def get_rds_instance_list_by_status(rds_instance_list, status) -> list:
        filtered_rds_instance_list = []

        for rds_instance in rds_instance_list:
            if ScheduleUtil.get_rds_instance_status(rds_instance) == status:
                filtered_rds_instance_list.append(rds_instance)

        return filtered_rds_instance_list

    @staticmethod
    def get_rds_instance_ids(rds_instance_list) -> list:
        rds_instance_ids = []

        for rds_instance in rds_instance_list:
            rds_instance_ids.append(rds_instance['DBInstanceIdentifier'])

        return rds_instance_ids

    @staticmethod
    def get_rds_instance_status(rds_instance) -> str:
        return rds_instance['DBInstanceStatus']

    @staticmethod
    def get_rds_instance_name(rds_instance, default_name='RDS') -> str:
        name = rds_instance['DBInstanceIdentifier']

        return default_name if not name else name

    @staticmethod
    def equals_rds_schedule_name(rds_tags, tag_value) -> bool:
        for t in rds_tags['TagList']:
            if t['Key'] != 'ScheduleName':
                continue
            if t['Value'] == tag_value:
                return True

        return False

    @staticmethod
    def get_diff_minute(d1, d2) -> float:
        diff = d1 - d2
        return (diff.days * 24 * 60) + (diff.seconds / 60)

    @staticmethod
    def hm_to_date_time(hm):
        cur = datetime.now()
        hour = hm.split(':')[0]
        minute = hm.split(':')[1]
        date_time = cur.replace(hour=int(hour), minute=int(minute), second=0, microsecond=0)

        return date_time

    @staticmethod
    def replace_time(date_time, hm):
        split_time = hm.split(':')
        hour = split_time[0]
        minute = split_time[1]
        return date_time.replace(hour=int(hour), minute=int(minute))

    @staticmethod
    def is_valid_time(hm):
        try:
            datetime.strptime(hm, '%H:%M')
            return True
        except ValueError:
            return False

    @staticmethod
    def is_valid_date(ymd):
        try:
            datetime.strptime(ymd, '%Y-%m-%d')
            return True
        except ValueError:
            return False

    @staticmethod
    def equals_rds_schedule_group_name(rds_tags, server_group) -> bool:
        for t in rds_tags['TagList']:
            if t['Key'] != 'ScheduleGroupName':
                continue
            if t['Value'] == server_group['GroupName']:
                return True

        return False


class JandiWebhook:
    color_err = '#FF0000'
    color_ok = '#1DDB16'
    color_warning = '#FFBB00'

    @staticmethod
    def build_connect_info(title, description=None, image_url=None):
        connect_info = {
            'title': title
        }

        if description is not None:
            connect_info['description'] = description

        if image_url is not None:
            connect_info['image_url'] = image_url

        return connect_info

    @staticmethod
    def build_message(msg, color, connect_info_list=None):
        payload = {
            'body': msg,
            'connectColor': color,
        }

        if connect_info_list is not None:
            payload['connectInfo'] = connect_info_list

        return payload

    @staticmethod
    def send_message(msg, color, connect_info_list=None):
        try:
            print("Send Webhook Message")

            headers = {
                'Accept': 'application/vnd.tosslab.jandi-v2+json',
                'Content-Type': 'application/json'
            }

            payload = json.dumps(JandiWebhook.build_message(msg, color, connect_info_list))

            print('body : ' + payload)

            res = requests.post(WEBHOOK_URL, headers=headers, data=payload)

            print('response : ' + str(res.status_code))
            res.raise_for_status()
        except Exception as e:
            print(e)

    @staticmethod
    def send_err_message(msg, connect_info_list=None):
        JandiWebhook.send_message(msg, JandiWebhook.color_err, connect_info_list)

    @staticmethod
    def send_warning_message(msg, connect_info_list=None):
        JandiWebhook.send_message(msg, JandiWebhook.color_warning, connect_info_list)

    @staticmethod
    def send_ok_message(msg, connect_info_list=None):
        JandiWebhook.send_message(msg, JandiWebhook.color_ok, connect_info_list)

    @staticmethod
    def send_start_rds_server_message(schedule, start_rds_instance_list):
        JandiWebhook.send_ok_message(
            '{0} 스케쥴 RDS 서버를 시작 합니다'.format(schedule['ScheduleName']),
            JandiWebhook.get_rds_server_connect_info_list('RDS 시작 서버 목록', start_rds_instance_list))

    @staticmethod
    def send_start_ec2_server_message(schedule, start_ec2_instance_list):
        JandiWebhook.send_ok_message(
            '{0} 스케쥴 EC2 서버를 시작 합니다'.format(schedule['ScheduleName']),
            JandiWebhook.get_ec2_server_connect_info_list('EC2 시작 서버 목록', start_ec2_instance_list))

    @staticmethod
    def send_start_rds_server_group_message(schedule, server_group, start_rds_instance_list):
        JandiWebhook.send_ok_message(
            '{0} 스케쥴 {1} 서버 그룹을 시작합니다'.format(schedule['ScheduleName'], server_group['GroupName']),
            JandiWebhook.get_rds_server_connect_info_list('RDS 시작 서버 목록', start_rds_instance_list))

    @staticmethod
    def send_start_ec2_server_group_message(schedule, server_group, start_ec2_instance_list):
        JandiWebhook.send_ok_message(
            '{0} 스케쥴의 {1} 서버 그룹을 시작합니다'.format(schedule['ScheduleName'], server_group['GroupName']),
            JandiWebhook.get_ec2_server_connect_info_list('EC2 시작 서버 목록', start_ec2_instance_list))

    @staticmethod
    def send_stop_ec2_server_message(schedule, stop_ec2_instance_list):
        JandiWebhook.send_ok_message(
            '{0} 스케쥴의 EC2 서버를 중지합니다'.format(schedule['ScheduleName']),
            JandiWebhook.get_ec2_server_connect_info_list('EC2 중지 서버 목록', stop_ec2_instance_list))

    @staticmethod
    def send_stop_rds_server_message(schedule, stop_rds_instance_list):
        JandiWebhook.send_ok_message(
            '{0} 스케쥴의 RDS 서버를 중지합니다'.format(schedule['ScheduleName']),
            JandiWebhook.get_rds_server_connect_info_list('RDS 중지 서버 목록', stop_rds_instance_list))

    @staticmethod
    def send_stop_alert_message(schedule_name, stop_date_time, remain):
        stop_alert_msg = '잠시후 {0} 스케쥴의 모든 서버가 중지 됩니다.'.format(schedule_name)
        stop_time_msg = JandiWebhook.build_connect_info('중지 시간', str(stop_date_time))
        remain_time_msg = JandiWebhook.build_connect_info('남은 시간', str(remain) + '분')
        JandiWebhook.send_warning_message(stop_alert_msg, [stop_time_msg, remain_time_msg])

    @staticmethod
    def send_exception_err_message(schedule, e, error_stack):
        connect_info = JandiWebhook.build_connect_info(str(e), error_stack)
        JandiWebhook.send_err_message('{0} 스케쥴에서 에러가 발생하였습니다'.format(schedule['ScheduleName']), [connect_info])

    @staticmethod
    def get_rds_server_connect_info_list(title, start_rds_instance_list) -> list:

        server_list = []

        for start_rds_instance in start_rds_instance_list:
            instance_name = start_rds_instance['DBInstanceIdentifier']
            engine = start_rds_instance['Engine']
            availability_zone = start_rds_instance['AvailabilityZone']
            server_list.append(instance_name + ' (' + engine + ') : ' + availability_zone)

        connect_info = JandiWebhook.build_connect_info(title, '\n'.join(server_list))

        return [connect_info]

    @staticmethod
    def get_ec2_server_connect_info_list(title, start_ec2_instance_list) -> list:

        server_list = []

        for start_ec2_instance in start_ec2_instance_list:
            instance_name = ScheduleUtil.get_ec2_instance_name(start_ec2_instance)
            availability_zone = start_ec2_instance['Placement']['AvailabilityZone']
            instance_id = start_ec2_instance['InstanceId']
            server_list.append(instance_name + ' (' + instance_id + ') : ' + availability_zone)

        connect_info = JandiWebhook.build_connect_info(title, '\n'.join(server_list))

        return [connect_info]


class Schedule:
    schedule_name = None
    schedule_data = {}

    db = boto3.resource('dynamodb')
    ec2 = boto3.client('ec2')
    rds = boto3.client('rds')

    def __init__(self, schedule_name):
        self.schedule_name = schedule_name

    def load_schedule_item_from_db(self):
        table = self.db.Table('Schedule')
        response = table.get_item(
            Key={
                'ScheduleName': self.schedule_name
            }
        )

        return response['Item'] if 'Item' in response else {}

    def get_schedule_property(self, property_name, default_value=None):
        if property_name in self.get_schedule():
            return self.get_schedule()[property_name]
        else:
            return default_value

    def get_schedule(self):
        if not self.schedule_data:
            self.schedule_data = self.load_schedule_item_from_db()

        return self.schedule_data

    def get_start_date_time(self) -> datetime:
        start_time = self.get_schedule_property('StartTime')

        if start_time == 'None':
            return None

        start_date_time = ScheduleUtil.hm_to_date_time(start_time)

        return start_date_time

    def get_stop_date_time(self) -> datetime:
        stop_time = self.get_schedule_property('StopTime')

        if stop_time == 'None':
            return None

        stop_date_time = ScheduleUtil.hm_to_date_time(stop_time)

        return stop_date_time

    def set_schedule_force_start(self, flag):
        return self.db.Table('Schedule').update_item(
            Key={
                'ScheduleName': self.schedule_name,
            },
            UpdateExpression='set ForceStart=:c',
            ExpressionAttributeValues={
                ':c': flag
            },
            ReturnValues='UPDATED_NEW'
        )

    def is_enable(self) -> bool:
        return self.get_schedule_property('Enabled')

    def is_active_day(self) -> bool:
        days_active = self.get_schedule_property('DaysActive')
        current_week_day = datetime.now().strftime('%a').lower()

        # 매일 작동
        if days_active == 'all':
            return True
        # 월~금만 작동
        elif days_active == 'weekdays':
            weekdays = ['mon', 'tue', 'wed', 'thu', 'fri']
            if current_week_day in weekdays:
                return True
        # 지정요일만 작동 (ex : mon,tue)
        else:
            days = days_active.split(',')
            for d in days:
                if d.lower().strip() == current_week_day:
                    return True

        return False

    def is_start_time(self) -> bool:
        if not self.is_active_day():
            return False

        start = self.get_start_date_time()

        if start is None:
            return False

        now = datetime.now()
        now_max = now - timedelta(minutes=59)

        return now_max <= start <= now

    def is_stop_time(self) -> bool:
        if not self.is_active_day():
            return False

        stop = self.get_stop_date_time()

        if stop is None:
            return False

        now = datetime.now()
        now_max = now - timedelta(minutes=59)

        return now_max <= stop <= now

    def is_force_start(self) -> bool:
        return self.get_schedule_property('ForceStart', False)

    def has_running_instance(self):
        ec2_instance_list = self.get_ec2_instance_list()
        rds_instance_list = self.get_rds_instance_list()

        running_ec2_instance_list = ScheduleUtil.get_ec2_instance_list_by_status(ec2_instance_list, 'running')
        running_rds_instance_list = ScheduleUtil.get_rds_instance_list_by_status(rds_instance_list, 'available')

        return False if len(running_ec2_instance_list) == 0 and len(running_rds_instance_list) == 0 else True

    def get_ec2_instance_list(self) -> list:

        schedule_tag_name = self.get_schedule_property('TagValue')

        ec2_schedule_filter = [{
            'Name': 'tag:ScheduleName',
            'Values': [schedule_tag_name]
        }]

        instances = self.ec2.describe_instances(Filters=ec2_schedule_filter)

        reservation_list = instances['Reservations']

        instance_list = []

        for reservation in reservation_list:
            instance_list = instance_list + reservation['Instances']

        return instance_list

    def get_rds_instance_list(self) -> list:

        schedule_tag_value = self.get_schedule_property('TagValue')
        instances = self.rds.describe_db_instances()

        schedule_instances_list = []

        for instance in instances['DBInstances']:
            arn = instance['DBInstanceArn']
            tags = self.rds.list_tags_for_resource(ResourceName=arn)

            if ScheduleUtil.equals_rds_schedule_name(tags, schedule_tag_value):
                schedule_instances_list.append(instance)

        return schedule_instances_list

    def start_ec2_instances(self, ec2_instance_list):
        ec2_instance_ids = ScheduleUtil.get_ec2_instance_ids(ec2_instance_list)
        return self.ec2.start_instances(InstanceIds=ec2_instance_ids)

    def stop_ec2_instances(self, ec2_instance_list):
        ec2_instance_ids = ScheduleUtil.get_ec2_instance_ids(ec2_instance_list)
        return self.ec2.stop_instances(InstanceIds=ec2_instance_ids, Force=True)

    def start_rds_instances(self, rds_instance_list):
        response_list = []
        rds_instance_ids = ScheduleUtil.get_rds_instance_ids(rds_instance_list)

        for rdb_instance_id in rds_instance_ids:
            res = self.rds.start_db_instance(DBInstanceIdentifier=rdb_instance_id)
            print('Start RDS :' + str(rdb_instance_id))
            response_list.append(res)

        return response_list

    def stop_rds_instances(self, rds_instance_list):
        response_list = []
        rds_instance_ids = ScheduleUtil.get_rds_instance_ids(rds_instance_list)

        for rdb_instance_id in rds_instance_ids:
            res = self.rds.stop_db_instance(DBInstanceIdentifier=rdb_instance_id)
            print('Stop RDS : ' + str(rdb_instance_id))
            response_list.append(res)

        return response_list

    def check_remain_stop_time(self):
        if not self.is_active_day():
            return

        stop_date_time = self.get_stop_date_time()

        if stop_date_time is None:
            return

        now = datetime.now()
        remain = int(round(ScheduleUtil.get_diff_minute(stop_date_time, now)))

        if not self.has_running_instance():
            return

        if 0 < remain <= STOP_ALERT_BEFORE_TIME_MINUTE:
            JandiWebhook.send_stop_alert_message(self.schedule_name, stop_date_time, remain)

    def start(self, is_force=False):

        if not self.is_start_time() and not is_force:
            return

        ec2_instance_list = self.get_ec2_instance_list()
        rds_instance_list = self.get_rds_instance_list()

        start_ec2_instance_list = ScheduleUtil.get_ec2_instance_list_by_status(ec2_instance_list, 'stopped')
        start_rds_instance_list = ScheduleUtil.get_rds_instance_list_by_status(rds_instance_list, 'stopped')

        if len(start_ec2_instance_list) > 0:
            try:
                self.start_ec2_instances(start_ec2_instance_list)
                JandiWebhook.send_start_ec2_server_message(self.get_schedule(), start_ec2_instance_list)
            except Exception as e:
                JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())

        if len(start_rds_instance_list) > 0:
            try:
                self.start_rds_instances(start_rds_instance_list)
                JandiWebhook.send_start_rds_server_message(self.get_schedule(), start_rds_instance_list)
            except Exception as e:
                JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())

        if is_force and len(start_ec2_instance_list) == 0 and len(start_rds_instance_list) == 0:
            self.set_schedule_force_start(False)

    def stop(self, is_force=False):

        if not self.is_stop_time() and not is_force:
            return

        ec2_instance_list = self.get_ec2_instance_list()
        rds_instance_list = self.get_rds_instance_list()

        stop_ec2_instance_list = ScheduleUtil.get_ec2_instance_list_by_status(ec2_instance_list, 'running')
        stop_rds_instance_list = ScheduleUtil.get_rds_instance_list_by_status(rds_instance_list, 'available')

        if len(stop_ec2_instance_list) > 0:
            try:
                self.stop_ec2_instances(stop_ec2_instance_list)
                JandiWebhook.send_stop_ec2_server_message(self.get_schedule(), stop_ec2_instance_list)
            except Exception as e:
                JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())

        if len(stop_rds_instance_list) > 0:
            try:
                JandiWebhook.send_stop_rds_server_message(self.get_schedule(), stop_rds_instance_list)
                self.stop_rds_instances(stop_rds_instance_list)
            except Exception as e:
                JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())

    def run(self):

        if self.is_force_start():
            self.start(True)
            return

        if not self.is_enable():
            return

        self.check_remain_stop_time()

        self.start()
        self.stop()


class GroupSchedule(Schedule):
    schedule_server_group_list = []

    def __init__(self, schedule_name):
        super().__init__(schedule_name)

    def load_schedule_server_group_list_from_db(self) -> list:
        table = self.db.Table('ScheduleServerGroup')

        response = table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Attr('ScheduleName').eq(self.schedule_name)
        )

        return response['Items']

    def get_schedule_server_group_list(self) -> list:
        if not self.schedule_server_group_list:
            self.schedule_server_group_list = self.load_schedule_server_group_list_from_db()

        return self.schedule_server_group_list

    def get_schedule_server_group(self, group_name):
        server_group_list = self.get_schedule_server_group_list()

        for server_group in server_group_list:
            if server_group['GroupName'] == group_name:
                return server_group

        return None

    def get_server_group_ec2_instance_list(self, server_group) -> list:
        schedule_tag_name = self.get_schedule_property('TagValue')

        ec2_schedule_filter = [{
            'Name': 'tag:ScheduleName',
            'Values': [schedule_tag_name]
        }, {
            'Name': 'tag:ScheduleGroupName',
            'Values': [server_group['GroupName']]
        }]
        instances = self.ec2.describe_instances(Filters=ec2_schedule_filter)

        reservation_list = instances['Reservations']

        instance_list = []

        for reservation in reservation_list:
            instance_list = instance_list + reservation['Instances']

        return instance_list

    def get_server_group_rds_instance_list(self, server_group) -> list:
        schedule_tag_value = self.get_schedule_property('TagValue')
        instances = self.rds.describe_db_instances()

        schedule_instances_list = []

        for instance in instances['DBInstances']:
            arn = instance['DBInstanceArn']
            tags = self.rds.list_tags_for_resource(ResourceName=arn)

            if False not in \
                    (ScheduleUtil.equals_rds_schedule_name(tags, schedule_tag_value),
                     ScheduleUtil.equals_rds_schedule_group_name(tags, server_group)):
                schedule_instances_list.append(instance)

        return schedule_instances_list

    def get_server_group_instance_list(self, server_group) -> list:
        if server_group is None:
            return []

        if server_group['InstanceType'] == 'RDS':
            return self.get_server_group_rds_instance_list(server_group)
        elif server_group['InstanceType'] == 'EC2':
            return self.get_server_group_ec2_instance_list(server_group)
        else:
            return []

    def has_server_group(self) -> bool:
        return False if not self.get_schedule_server_group_list() else True

    def is_server_group_running(self, server_group) -> bool:
        instance_list = self.get_server_group_instance_list(server_group)

        if len(instance_list) is 0:
            return True
        else:
            for instance in instance_list:
                if server_group['InstanceType'] == 'EC2':
                    if ScheduleUtil.get_ec2_instance_status(instance) != 'running':
                        return False
                elif server_group['InstanceType'] == 'RDS':
                    if ScheduleUtil.get_rds_instance_status(instance) != 'available':
                        return False

            return True

    def is_dependency_server_group_all_running(self, server_group) -> bool:
        dependency_list = server_group['Dependency']

        if len(dependency_list) > 0:
            for dependency in dependency_list:
                dependency_server_group = self.get_schedule_server_group(dependency)

                if not self.is_server_group_running(dependency_server_group):
                    return False

        return True

    def start_server_group_instance(self, server_group):
        if server_group['InstanceType'] == 'EC2':
            ec2_instance_list = self.get_server_group_ec2_instance_list(server_group)
            start_ec2_instance_list = ScheduleUtil.get_ec2_instance_list_by_status(ec2_instance_list, 'stopped')

            if len(start_ec2_instance_list) > 0:
                try:
                    JandiWebhook.send_start_ec2_server_group_message(self.get_schedule(), server_group,
                                                                     ec2_instance_list)
                    start_ec2_response = self.start_ec2_instances(start_ec2_instance_list)
                    return start_ec2_response
                except Exception as e:
                    JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())

        elif server_group['InstanceType'] == 'RDS':
            rds_instance_list = self.get_server_group_rds_instance_list(server_group)
            start_rds_instance_list = ScheduleUtil.get_rds_instance_list_by_status(rds_instance_list, 'stopped')

            if len(start_rds_instance_list) > 0:
                try:
                    JandiWebhook.send_start_rds_server_group_message(self.get_schedule(), server_group,
                                                                     start_rds_instance_list)
                    start_rds_response_list = self.start_rds_instances(start_rds_instance_list)
                    return start_rds_response_list
                except Exception as e:
                    JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())

    def stop_server_group_instance(self, server_group):
        if server_group['InstanceType'] == 'EC2':
            ec2_instance_list = self.get_server_group_ec2_instance_list(server_group)
            stop_ec2_instance_list = ScheduleUtil.get_ec2_instance_list_by_status(ec2_instance_list, 'running')
            self.stop_ec2_instances(stop_ec2_instance_list)

        elif server_group['InstanceType'] == 'RDS':
            rds_instance_list = self.get_server_group_ec2_instance_list(server_group)
            stop_rds_instance_list = ScheduleUtil.get_rds_instance_list_by_status(rds_instance_list, 'available')
            self.stop_rds_instances(stop_rds_instance_list)

    def start(self, is_force=False):

        if not self.get_schedule_server_group_list():
            super().start(is_force)
            return

        if not self.is_start_time() and not is_force:
            return

        is_all_server_group_running = True
        server_group_list = self.get_schedule_server_group_list()

        for server_group in server_group_list:
            is_dependency_started = self.is_dependency_server_group_all_running(server_group)

            if is_dependency_started:
                try:
                    self.start_server_group_instance(server_group)
                except Exception as e:
                    JandiWebhook.send_exception_err_message(self.get_schedule(), e, traceback.format_exc())
            else:
                is_all_server_group_running = False
                print(server_group['GroupName'] + ' 의 의존관계 ' + str(server_group['Dependency']) + ' 가 아직 시작하지 않았습니다')

        if is_force and is_all_server_group_running:
            self.set_schedule_force_start(False)


class ExceptionSchedule(GroupSchedule):
    schedule_exception_list = None

    def __init__(self, schedule_name):
        super().__init__(schedule_name)

    def load_schedule_exception_list_from_db(self):
        schedule_date_ymd = self.get_exception_date_ymd()
        table = self.db.Table('ScheduleException')
        response = table.scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Attr('ScheduleName').eq(self.schedule_name) & Attr('ExceptionDate').eq(schedule_date_ymd)
        )

        return response['Items']

    def get_exception_date_ymd(self) -> str:
        return datetime.now().strftime('%Y-%m-%d')

    def get_schedule_exception_list(self) -> list:
        if self.schedule_exception_list is None:
            self.schedule_exception_list = self.load_schedule_exception_list_from_db()

        return [] if self.schedule_exception_list is None else self.schedule_exception_list

    def get_exception_value(self, exception_type):
        exception_list = self.get_schedule_exception_list()

        for exception in exception_list:
            if exception['ExceptionType'] == exception_type:
                return exception['ExceptionValue']

        return None

    def get_start_date_time(self):
        origin_start_date_time = super(ExceptionSchedule, self).get_start_date_time()

        start_exception_value = self.get_exception_value('start')

        if start_exception_value is None:
            return origin_start_date_time
        elif start_exception_value == 'None':
            return None
        else:
            if origin_start_date_time is None:
                return ScheduleUtil.hm_to_date_time(start_exception_value)
            else:
                return ScheduleUtil.replace_time(origin_start_date_time, start_exception_value)

    def get_stop_date_time(self):
        origin_stop_date_time = super(ExceptionSchedule, self).get_stop_date_time()

        stop_exception_value = self.get_exception_value('stop')

        if stop_exception_value is None:
            return origin_stop_date_time
        elif stop_exception_value == 'None':
            return None
        else:
            if origin_stop_date_time is None:
                return ScheduleUtil.hm_to_date_time(stop_exception_value)
            else:
                return ScheduleUtil.replace_time(origin_stop_date_time, stop_exception_value)

    def set_schedule_exception(self, exception_date, exception_type, exception_value):
        scan_items = self.scan_schedule_exception(exception_date, exception_type)
        is_already = True if len(scan_items) > 0 else False

        if is_already:
            self.update_schedule_exception(scan_items[0], exception_value)
        else:
            self.insert_schedule_exception(exception_date, exception_type, exception_value)

    def remove_schedule_exception(self, exception_date, exception_type):
        scan_items = self.scan_schedule_exception(exception_date, exception_type)

        for item in scan_items:
            self.delete_schedule_exception(item)

    def scan_schedule_exception(self, exception_date, exception_type):
        scan_result = self.db.Table('ScheduleException').scan(
            Select='ALL_ATTRIBUTES',
            FilterExpression=Attr('ScheduleName').eq(self.schedule_name) & Attr('ExceptionDate').eq(
                exception_date.strftime('%Y-%m-%d')) & Attr('ExceptionType').eq(exception_type)
        )

        return scan_result['Items']

    def insert_schedule_exception(self, exception_date, exception_type, exception_value):
        new_uuid = str(uuid.uuid4())

        return self.db.Table('ScheduleException').put_item(
            Item={
                'ExceptionUuid': new_uuid,
                'ExceptionDate': exception_date.strftime('%Y-%m-%d'),
                'ExceptionType': exception_type,
                'ExceptionValue': exception_value,
                'ScheduleName': self.schedule_name
            }
        )

    def update_schedule_exception(self, item, value):
        return self.db.Table('ScheduleException').update_item(
            Key={
                'ExceptionUuid': item['ExceptionUuid'],
            },
            UpdateExpression='set ExceptionValue=:c',
            ExpressionAttributeValues={
                ':c': value
            },
            ReturnValues='UPDATED_NEW'
        )

    def delete_schedule_exception(self, item):
        return self.db.Table('ScheduleException').delete_item(
            Key={
                'ExceptionUuid': item['ExceptionUuid'],
            }
        )

    def print_schedule_data(self):

        print('========================================================================')
        print('Schedule Name : ' + self.schedule_name)
        print('Today : ' + self.get_exception_date_ymd())
        print('Enabled : ' + str(self.is_enable()))
        print('ActiveDays : ' + str(self.get_schedule_property('DaysActive')))
        print('Start Time : ' + str(self.get_start_date_time()))
        print('Stop Time : ' + str(self.get_stop_date_time()))
        print('IsActiveDay : ' + str(self.is_active_day()))
        print('IsStartTime : ' + str(self.is_start_time()))
        print('IsStopTime : ' + str(self.is_stop_time()))
        print('TagValue : ' + str(self.get_schedule_property('TagValue')))
        print('========================================================================')

    def print_schedule_group_data(self):

        group_list = self.get_schedule_server_group_list()

        for group in group_list:
            il = self.get_server_group_instance_list(group)
            gn = group['GroupName']
            print('------------------------------------------------------------------------')
            print(gn + ' -> ' + str(group['Dependency']))
            print('------------------------------------------------------------------------')
            for i in il:
                if gn == 'RDB':
                    print(i['DBInstanceIdentifier'])
                else:
                    print(ScheduleUtil.get_ec2_instance_name(i))


class SpecificDateSchedule(ExceptionSchedule):

    today_ymd = None

    def __init__(self, schedule_name, today_ymd):
        super().__init__(schedule_name)
        self.today_ymd = today_ymd

    def get_exception_date_ymd(self) -> str:
        return datetime.strptime(self.today_ymd, '%Y-%m-%d').strftime('%Y-%m-%d')


class Scheduler:

    @staticmethod
    def run_job():
        db = boto3.resource('dynamodb')
        table = db.Table('Schedule')
        response = table.scan()

        for item in response['Items']:
            schedule = ExceptionSchedule(item['ScheduleName'])
            schedule.print_schedule_data()
            schedule.run()
            Scheduler.print_line()

    @staticmethod
    def print_schedules():
        db = boto3.resource('dynamodb')
        table = db.Table('Schedule')
        response = table.scan()

        for item in response['Items']:
            schedule = ExceptionSchedule(item['ScheduleName'])
            schedule.print_schedule_data()
            schedule.print_schedule_group_data()
            Scheduler.print_line()

    @staticmethod
    def print_line():
        print('\n----------------------------------------------------------------------------------------------\n')


class BotError(Exception):

    bot = None

    def __init__(self, value, bot):
        self.bot = bot
        self.value = value

    def err_response(self):
        connect_info = JandiWebhook.build_connect_info("Error Stack", traceback.format_exc())
        return SchedulerBot.build_http_response(
            JandiWebhook.build_message(self.__str__(), JandiWebhook.color_err, [connect_info]))


class BotInvalidError(BotError):
    def __init__(self, value, bot):
        super(BotInvalidError, self).__init__(value, bot)

    def __str__(self):
        return str(self.value)

    def err_response(self):
        return SchedulerBot.build_http_response(
            JandiWebhook.build_message(self.__str__(), JandiWebhook.color_err))


class BotCommandSyntaxError(BotError):

    description = None

    def __init__(self, value, bot, description=None):
        super(BotCommandSyntaxError, self).__init__(value, bot)
        self.description = description

    def err_response(self):
        connect_info = {} if not self.description else \
            JandiWebhook.build_connect_info(str(self), self.description)

        return SchedulerBot.build_http_response(
            JandiWebhook.build_message('잘 못들었습니다?', JandiWebhook.color_err, [connect_info]))


class SchedulerBot:

    event = None
    body = None
    schedule = None

    def __init__(self, event) -> None:
        super().__init__()
        self.event = event
        self.body = json.loads(self.event['body'])
        print(self.body)

    def run(self):
        try:
            if not self.is_valid_token():
                return self.wrong_response('손들어 움직이면 쏜다!')

            command_result = self.command()

            if type(command_result) == str:
                return self.build_http_response(JandiWebhook.build_message(command_result, JandiWebhook.color_ok))
            else:
                return self.build_http_response(command_result)

        except BotError as be:
            return be.err_response()

        except Exception as e:
            print(traceback.format_exc())
            return self.not_handle_err_response(e, traceback.format_exc())

    def is_valid_token(self) -> bool:
        print('token : ' + self.body['token'])
        print('outgoing : ' + OUTGOING_WEBHOOK_TOKEN)
        return True if self.body['token'] == OUTGOING_WEBHOOK_TOKEN else False

    def command(self):
        hook_text = self.body['text']
        args = hook_text.split()[1:]

        if not args:
            raise BotCommandSyntaxError('Command parsing error', self)

        # 도움말
        if args[0] == 'help':
            return self.help()
        else:
            return self.command_schedule(args)

    def command_schedule(self, args):
        schedule_name = args[0]

        self.schedule = ExceptionSchedule(schedule_name)

        # schedule 유효성 체크
        if not self.schedule.get_schedule():
            raise BotInvalidError('**{0}** 은 저희 부대 스케쥴이 아닌거 같지 말입니다'.format(schedule_name), self)

        command = args[1]

        if command == 'status' or command == 's':
            return self.status()
        elif command == 'info' or command == 'i':
            return self.info(args[2:])
        elif command == 'exception' or command == 'e':
            return self.exception(args[2:])
        elif command == 'force_start':
            return self.force_start()
        elif command == 'force_stop':
            return self.force_stop()
        else:
            raise BotCommandSyntaxError('Wrong command', self)

    def help(self):

        keyword = self.body['keyword']

        help_command_list = [
            '/{0} help : 도움말'.format(keyword),
            '/{0} [스케쥴명] status : 현재 서버 상태 조회'.format(keyword),
            '/{0} [스케쥴명] info : 오늘의 스케쥴 조회'.format(keyword),
            '/{0} [스케쥴명] info [YYYY-MM-DD] : 특정일 스케쥴 조회'.format(keyword),
            '/{0} [스케쥴명] exception info : 오늘의 스케쥴 예외 조회'.format(keyword),
            '/{0} [스케쥴명] exception info [YYYY-MM-DD] : 특정일 스케쥴 예외 조회'.format(keyword),
            '/{0} [스케쥴명] exception set [YYYY-MM-DD] [start|stop] [h:m] : 예외 설정'.format(keyword),
            '/{0} [스케쥴명] exception del [YYYY-MM-DD] [start|stop] : 예외 삭제'.format(keyword),
            '/{0} [스케쥴명] force_start : 서버 강제실행'.format(keyword),
            '/{0} [스케쥴명] force_stop : 서버 강제중지'.format(keyword)]

        connect_info = JandiWebhook.build_connect_info("명령 커멘드", '\n'.join(help_command_list))

        return JandiWebhook.build_message('충성!', JandiWebhook.color_ok, [connect_info])

    def status(self):

        has_server_group = self.schedule.has_server_group()

        if has_server_group:
            info = self.get_server_group_status_connect_info_list()
        else:
            info = self.get_server_status_connect_info_list()

        return JandiWebhook.build_message(
            '보고 합니다! 현재 서버 상태는 총 : **{0}**, 작업 : **{1}**, 열외 : **{2}** 이상!'.format(
                info['on'] + info['off'], info['on'], info['off']),
            JandiWebhook.color_ok, info['connect_info_list'])

    def get_server_group_status_connect_info_list(self):

        result = {}
        on = 0
        off = 0
        connect_info_list = []
        server_group_list = self.schedule.get_schedule_server_group_list()

        for server_group in server_group_list:
            instance_list = self.schedule.get_server_group_instance_list(server_group)

            description_list = []

            for instance in instance_list:

                instance_type = server_group['InstanceType']

                if instance_type == 'EC2':
                    status = ScheduleUtil.get_ec2_instance_status(instance)
                    description_list.append('{0} : {1}'.format(
                            ScheduleUtil.get_ec2_instance_name(instance),
                            status))

                    if status == 'running':
                        on += 1
                    else:
                        off += 1

                elif instance_type == 'RDS':
                    status = ScheduleUtil.get_rds_instance_status(instance)
                    description_list.append('{0} : {1}'.format(
                            ScheduleUtil.get_rds_instance_name(instance),
                            status))

                    if status == 'available':
                        on += 1
                    else:
                        off += 1
                else:
                    continue

            connect_info_list.append(
                JandiWebhook.build_connect_info(
                    '{0} 그룹의 서버 상태'.format(server_group['GroupName']),
                    '\n'.join(description_list)))

        result['on'] = on
        result['off'] = off
        result['connect_info_list'] = connect_info_list

        return result

    def get_server_status_connect_info_list(self):
        result = {}
        on = 0
        off = 0
        connect_info_list = []
        rds_instance_list = self.schedule.get_rds_instance_list()
        ec2_instance_list = self.schedule.get_ec2_instance_list()

        if len(rds_instance_list) > 0:

            rds_desc_list = []

            for rds_instance in rds_instance_list:
                status = ScheduleUtil.get_rds_instance_status(rds_instance)
                rds_desc_list.append('{0} : {1}'.format(
                        ScheduleUtil.get_rds_instance_name(rds_instance),
                        status))

                if status == 'available':
                    on += 1
                else:
                    off += 1

            connect_info_list.append(
                JandiWebhook.build_connect_info(
                    '{0} 서버 상태'.format('RDS'),
                    '\n'.join(rds_desc_list)))

        if len(ec2_instance_list) > 0:

            ec2_desc_list = []

            for ec2_instance in ec2_instance_list:
                status = ScheduleUtil.get_ec2_instance_status(ec2_instance)
                ec2_desc_list.append('{0} : {1}'.format(
                        ScheduleUtil.get_ec2_instance_name(ec2_instance),
                        status))

                if status == 'running':
                    on += 1
                else:
                    off += 1

            connect_info_list.append(
                JandiWebhook.build_connect_info(
                    '{0} 서버 상태'.format('EC2'),
                    '\n'.join(ec2_desc_list)))

        result['on'] = on
        result['off'] = off
        result['connect_info_list'] = connect_info_list

        return result

    def info(self, args):
        if len(args) > 0:
            dt = args[0]
            if not ScheduleUtil.is_valid_date(dt):
                raise BotInvalidError('날짜 형식을 잘못 입력하였습니다', self)

            desc = self.build_info_by_schedule(SpecificDateSchedule(self.schedule.schedule_name, dt))

            return JandiWebhook.build_message("**{0}** 스케쥴 정보 입니다!".format(dt), JandiWebhook.color_ok, [desc])

        else:
            desc = self.build_info_by_schedule(self.schedule)

            return JandiWebhook.build_message("오늘의 스케쥴 정보 입니다!", JandiWebhook.color_ok, [desc])

    def build_info_by_schedule(self, schedule_obj):
        desc_list = [
            'Enabled : {0}'.format(schedule_obj.is_enable()),
            'DaysActive : {0}'.format(schedule_obj.get_schedule_property('DaysActive')),
            'Start Time : {0}'.format(schedule_obj.get_start_date_time().strftime('%H:%M')
                                      if schedule_obj.get_start_date_time() is not None else 'None'),
            'Stop Time : {0}'.format(schedule_obj.get_stop_date_time().strftime('%H:%M')
                                     if schedule_obj.get_stop_date_time() is not None else 'None'),
        ]

        return JandiWebhook.build_connect_info("상세정보", '\n'.join(desc_list))

    def exception(self, args):
        if len(args) == 0:
            raise BotCommandSyntaxError('Exception command error', self)
        elif args[0] == 'info' or args[0] == 'i':
            return self.exception_info(args[1:])
        elif args[0] == 'set' or args[0] == 's':
            return self.exception_set(args[1:])
        elif args[0] == 'del' or args[0] == 'd':
            return self.exception_del(args[1:])
        else:
            raise BotCommandSyntaxError('Wrong exception command error', self)

    def exception_info(self, args):
        if len(args) > 0:
            dt = args[0]
            if not ScheduleUtil.is_valid_date(dt):
                raise BotInvalidError('날짜 형식을 잘못 입력하였습니다', self)

            return self.build_exception_info_by_schedule(SpecificDateSchedule(self.schedule.schedule_name, dt), dt)
        else:
            return self.build_exception_info_by_schedule(self.schedule)

    def build_exception_info_by_schedule(self, schedule_obj, dt=None):
        exception_list = schedule_obj.get_schedule_exception_list()

        date = '오늘' if dt is None else dt

        if len(exception_list) == 0:
            return '**{0}** 근무시간 변경이 없습니다'.format(date)

        desc_list = []

        for exception in exception_list:
            desc_list.append('**{0}** 시간을 **{1}**으로 변경'.format(exception['ExceptionType'], exception['ExceptionValue']))

        return JandiWebhook.build_message('**{0}** 근무시간 변경표 입니다'.format(date), JandiWebhook.color_ok,
                                          [JandiWebhook.build_connect_info('변경표', '\n'.join(desc_list))])

    def exception_set(self, args):
        if not len(args) == 3:
            raise BotCommandSyntaxError('Wrong exception set error', self)

        exception_date = args[0]
        exception_type = args[1]
        exception_time = args[2]

        if not ScheduleUtil.is_valid_date(exception_date):
            raise BotInvalidError('날짜가 잘못되었지 말입니다', self)

        if not ScheduleUtil.is_valid_time(exception_time):
            raise BotInvalidError('시간이 잘못되었지 말입니다', self)

        exception_date = datetime.strptime(exception_date, '%Y-%m-%d').date()

        h = int(exception_time.split(':')[0])

        self.schedule.set_schedule_exception(exception_date, exception_type, exception_time)

        result = []

        if h > 20:
            result.append("야간 근무 하십니까? 고생하시지 말입니다!")

        result.append('**{0}** 일 **{1}** 스케쥴 **{2}** 시간은 **{3}** 으로 설정되었습니다'.format(
            exception_date,
            self.schedule.schedule_name,
            exception_type,
            exception_time))

        return '\n'.join(result)

    def exception_del(self, args):

        if not len(args) == 2:
            raise BotCommandSyntaxError('Wrong exception del error', self)

        exception_date = args[0]
        exception_type = args[1]

        if not ScheduleUtil.is_valid_date(exception_date):
            raise BotInvalidError('날짜가 잘못되었지 말입니다', self)

        exception_date = datetime.strptime(exception_date, '%Y-%m-%d').date()

        self.schedule.remove_schedule_exception(exception_date, exception_type)

        result = exception_date.strftime('%Y-%m-%d') + ' 일 ' + exception_type + ' 예외를 삭제 하였습니다.'

        return result

    def force_start(self):

        self.schedule.set_schedule_force_start(True)

        return '진돗개 하나 발령! 현 시간부로 모든 서버들은 기상한다!'

    def force_stop(self):

        self.schedule.set_schedule_force_start(False)

        self.schedule.stop(True)

        return '취침소등 하겠습니다!'

    @staticmethod
    def build_http_response(res=None):
        return {
            'statusCode': '200',
            'body': {} if not res else json.dumps(res),
            'headers': {
                'Content-Type': 'application/json',
            },
        }

    def wrong_response(self, wrong_message):
        return self.build_http_response(JandiWebhook.build_message(wrong_message, JandiWebhook.color_warning))

    def not_handle_err_response(self, exception, error_stack):
        connect_info = JandiWebhook.build_connect_info(str(exception), error_stack)
        return self.build_http_response(JandiWebhook.build_message('검열한번 하셔야 할거 같지 말입니다',
                                                                   JandiWebhook.color_err, [connect_info]))


def handle(event, context):

    if event and 'httpMethod' in event:
        bot = SchedulerBot(event)
        res = bot.run()
        print(res)
        return res

    else:
        return Scheduler.run_job()
