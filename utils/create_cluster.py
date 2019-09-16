#!/usr/bin/env python3

import configparser
import json
import sys
import time

import boto3

ecs = boto3.client('ecs')

# Read AWS credentials
config = configparser.ConfigParser()
config.read_file(open('../dwh.cfg'))

# Set AWS credentials and region
KEY = config.get('AWS', 'KEY')
SECRET = config.get('AWS', 'SECRET')
REGION_NAME = config.get('AWS', 'REGION_NAME')

# Set cluster provisioning details
DWH_CLUSTER_TYPE = config.get('DWH', "DWH_CLUSTER_TYPE")
DWH_NUM_NODES = config.get('DWH', "DWH_NUM_NODES")
DWH_NODE_TYPE = config.get('DWH', "DWH_NODE_TYPE")
DWH_CLUSTER_IDENTIFIER = config.get('DWH', "DWH_CLUSTER_IDENTIFIER")
DWH_DB = config.get('DWH', "DWH_DB")
DWH_DB_USER = config.get('DWH', "DWH_DB_USER")
DWH_DB_PASSWORD = config.get('DWH', "DWH_DB_PASSWORD")
DWH_PORT = config.get('DWH', "DWH_PORT")
DWH_IAM_ROLE_NAME = config.get('DWH', "DWH_IAM_ROLE_NAME")

# create clients to interact with aws
ec2 = boto3.resource('ec2', region_name=REGION_NAME, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
iam = boto3.client('iam', region_name=REGION_NAME, aws_access_key_id=KEY, aws_secret_access_key=SECRET)
redshift = boto3.client('redshift', region_name=REGION_NAME, aws_access_key_id=KEY, aws_secret_access_key=SECRET)


def create_iam_role(role_name):
    """
    Creates an IAM role with Redshift permissions
    :param role_name: the name to be given to the role
    :return: None
    """
    try:
        print('1.1 Creating a new IAM Role')
        iam.create_role(
            Path='/',
            RoleName=role_name,
            Description="Grant Redshift S3 read access",
            AssumeRolePolicyDocument=json.dumps({'Statement': [
                {'Action': 'sts:AssumeRole',
                 'Effect': 'Allow',
                 'Principal': {'Service': 'redshift.amazonaws.com'}}],
                'Version': '2012-10-17'})
        )

    except Exception as e:
        print(e)


def attach_s3_and_redshift_policies(role_name):
    """
    Attaches S3 read only and Redshift read only to role
    :param role_name: the name of the role to give access to
    :return:
    """
    try:
        print('1.2 Attaching Policy')
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess')
        iam.attach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonRedshiftReadOnlyAccess')

    except Exception as e:
        print(e)


def get_arn(role_name):
    """
    Gets the ARN for a given role
    :param role_name: the name of the rold
    :return:
    """
    try:
        print('1.3 Get the IAM role ARN')
        response = iam.get_role(RoleName=role_name)
        return response['Role']['Arn']

    except Exception as e:
        print(e)


def create_cluster(role_arn):
    """
    Creates a cluster in Redshift.
    :param role_arn: the IAM role being granted access
    :return: create cluster response
    """
    # Num nodes must not be included when instantiating single-node "clusters"
    # If num_nodes is 1, then we don't pass this parameter to the boto client
    if int(DWH_NUM_NODES) > 1:
        try:
            response = redshift.create_cluster(
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                NumberOfNodes=int(DWH_NUM_NODES),
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                IamRoles=[role_arn]
            )
            print(f'Creating {DWH_CLUSTER_TYPE} cluster with {DWH_NUM_NODES} nodes named {DWH_CLUSTER_IDENTIFIER}')
            return response

        except Exception as e:
            print(e)
    else:
        try:
            response = redshift.create_cluster(
                ClusterType=DWH_CLUSTER_TYPE,
                NodeType=DWH_NODE_TYPE,
                DBName=DWH_DB,
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                MasterUsername=DWH_DB_USER,
                MasterUserPassword=DWH_DB_PASSWORD,
                IamRoles=[role_arn]
            )
            print(f'Creating {DWH_CLUSTER_TYPE} cluster with named {DWH_CLUSTER_IDENTIFIER}')
            return response

        except Exception as e:
            print(e)


def wait_for_cluster(cluster_identifier, target_status, interval=30):
    """
    Waits for a cluster to finish creaating or deleting, then returns control to the caller
    :param cluster_identifier: the redshift cluster identifier, i.e. name
    :param target_status: the desired outcome; i.e., 'available' or 'deleted'
    :param interval: how long to sleep while waiting
    :return: Endpoint and role ARN if creating a cluster. If deleting, return None
    """
    cluster_properties = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    cluster_status = cluster_properties['ClusterStatus']

    while cluster_status != target_status:
        print(f'Current cluster status is {cluster_status}. Sleeping for 60s')
        time.sleep(interval)
        try:
            cluster_properties = redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
            cluster_status = cluster_properties['ClusterStatus']
        except ecs.exceptions.ClusterNotFoundException as e:
            print(e)

    print(f'Current cluster status is {cluster_status}. Exiting.')
    if cluster_status == 'available':
        endpoint, role_arn = cluster_properties['Endpoint']['Address'], cluster_properties['IamRoles'][0]['IamRoleArn']
        print(f"DWH_ENDPOINT: {endpoint}")
        print(f"DWH_ROLE_ARN: {role_arn}")
        return cluster_properties, endpoint, role_arn
    return None


def open_port(my_cluster_properties, ip_address='0.0.0.0/0', protocol='tcp', port=5439):
    """
    Open an incoming TCP port to access the cluster endpoint. By default allows tcp traffic from any host on 5439
    :return: None
    """
    try:
        vpc = ec2.Vpc(id=my_cluster_properties['VpcId'])
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)

        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp=ip_address,
            IpProtocol=protocol,
            FromPort=int(port),
            ToPort=int(port)
        )
    except Exception as e:
        print(e)


def describe_cluster(cluster_identifier):
    """
    Gives the status of a cluster
    :param cluster_identifier: the cluster identifier
    :return: None
    """
    try:
        return redshift.describe_clusters(ClusterIdentifier=cluster_identifier)['Clusters'][0]
    except Exception as e:
        print(e)


def detach_role_policy(role_name):
    """
    Detaches S3 read only access role policy from a given role
    :param role_name: the iam role name
    :return: None
    """
    try:
        iam.detach_role_policy(RoleName=role_name, PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
        print(f'Detaching AmazonS3ReadOnlyAccess from {role_name}')
    except Exception as e:
        print(e)
    try:
        iam.detach_role_policy(RoleName=role_name, PolicyArn='arn:aws:iam::aws:policy/AmazonRedshiftReadOnlyAccess')
        print(f'Detaching AmazonRedshiftReadOnlyAccess from {role_name}')
    except Exception as e:
        print(e)


def delete_role(role_name):
    """
    Deletes an IAM role
    :param role_name: role_name: the iam role name
    :return: None
    """
    try:
        iam.delete_role(RoleName=role_name)
        print(f'Deleting {role_name}')
    except Exception as e:
        print(e)


def delete_cluster(cluster_identifier, skip_snapshot=True):
    """
    Deletes a cluster. Does not take a snapshot by default
    :param cluster_identifier: the cluster identifier
    :param skip_snapshot: False if you want to keep a snapshot
    :return: None
    """
    try:
        redshift.delete_cluster(ClusterIdentifier=cluster_identifier, SkipFinalClusterSnapshot=skip_snapshot)
        print(f"Deleting cluster {cluster_identifier}")
    except Exception as e:
        print(e)


def clean_up_cluster_and_role(wait=True, skip_snapshot=True, interval=30):
    """Deletes the cluster and role created by this script
    :param skip_snapshot: False if you want to keep a snapshot
    :param wait: True reports on the status of deletion and hold execution
    :param interval: True reports on the status of deletion and hold execution
    :return: None
    """
    delete_cluster(DWH_CLUSTER_IDENTIFIER, skip_snapshot)
    if wait:
        try:
            wait_for_cluster(DWH_CLUSTER_IDENTIFIER, 'deleted', interval)
        except Exception as e:
            print(e)
    detach_role_policy(DWH_IAM_ROLE_NAME)
    delete_role(DWH_IAM_ROLE_NAME)


def main():
    """Creates an iam role, attaches s3 and redshift read only policies to it, stands up a redshift cluster, and opens
    a port so it can be accessed."""
    create_iam_role(DWH_IAM_ROLE_NAME)
    attach_s3_and_redshift_policies(DWH_IAM_ROLE_NAME)
    role_arn = get_arn(DWH_IAM_ROLE_NAME)
    create_cluster(role_arn)
    wait_for_cluster(DWH_CLUSTER_IDENTIFIER, 'available')
    cluster_props, endpoint, role_arn = wait_for_cluster(DWH_CLUSTER_IDENTIFIER, target_status='available')
    open_port(cluster_props)


if __name__ == '__main__':
    try:
        if sys.argv[1] == 'create':
            main()
        elif sys.argv[1] == 'delete':
            clean_up_cluster_and_role()
        else:
            print('Arg must be one of create|delete')
            print('To create a cluster from dwh.cfg: create_cluster.py create')
            print('To delete the cluster from dwh.cfg: create_cluster.py delete')
            sys.exit(1)
    except IndexError:
        print('Usage: python3 create_cluster [create|delete]')
        print('Arg must be one of create|delete')
        print('Exiting')
        sys.exit(1)
