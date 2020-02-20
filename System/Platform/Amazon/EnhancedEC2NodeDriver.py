import base64

from libcloud.compute.drivers.ec2 import EC2NodeDriver


class EnhancedEC2NodeDriver(EC2NodeDriver):

    def __init__(self, key, secret=None, secure=True, host=None, port=None,
                 region='us-east-1', token=None, **kwargs):

        super(EnhancedEC2NodeDriver, self).__init__(key, secret, secure, host, port, region, token, **kwargs)

    def create_node(self, **kwargs):
        image = kwargs["image"]
        size = kwargs["size"]
        params = {
            'Action': 'RunInstances',
            'ImageId': image.id,
            'MinCount': str(kwargs.get('ex_mincount', '1')),
            'MaxCount': str(kwargs.get('ex_maxcount', '1')),
            'InstanceType': size.id
        }

        if kwargs.get("ex_spot_market", False):
            params["InstanceMarketOptions.MarketType"] = "spot"
            if kwargs.get("ex_spot_price", False):
                params["InstanceMarketOptions.SpotOptions.MaxPrice"] =\
                    str(kwargs.get("ex_spot_price"))

        if kwargs.get("ex_terminate_on_shutdown", False):
            params["InstanceInitiatedShutdownBehavior"] = "terminate"

        if 'ex_security_groups' in kwargs and 'ex_securitygroup' in kwargs:
            raise ValueError('You can only supply ex_security_groups or'
                             ' ex_securitygroup')

        # ex_securitygroup is here for backward compatibility
        ex_security_groups = kwargs.get('ex_security_groups', None)
        ex_securitygroup = kwargs.get('ex_securitygroup', None)
        security_groups = ex_security_groups or ex_securitygroup

        if security_groups:
            if not isinstance(security_groups, (tuple, list)):
                security_groups = [security_groups]

            for sig in range(len(security_groups)):
                params['SecurityGroup.%d' % (sig + 1,)] = \
                    security_groups[sig]

        if 'ex_security_group_ids' in kwargs and 'ex_subnet' not in kwargs:
            raise ValueError('You can only supply ex_security_group_ids'
                             ' combinated with ex_subnet')

        security_group_ids = kwargs.get('ex_security_group_ids', None)
        security_group_id_params = {}

        if security_group_ids:
            if not isinstance(security_group_ids, (tuple, list)):
                security_group_ids = [security_group_ids]

            for sig in range(len(security_group_ids)):
                security_group_id_params['SecurityGroupId.%d' % (sig + 1,)] = \
                    security_group_ids[sig]

        if 'location' in kwargs:
            availability_zone = getattr(kwargs['location'],
                                        'availability_zone', None)
            if availability_zone:
                if availability_zone.region_name != self.region_name:
                    raise AttributeError('Invalid availability zone: %s'
                                         % (availability_zone.name))
                params['Placement.AvailabilityZone'] = availability_zone.name

        if 'auth' in kwargs and 'ex_keyname' in kwargs:
            raise AttributeError('Cannot specify auth and ex_keyname together')

        if 'auth' in kwargs:
            auth = self._get_and_check_auth(kwargs['auth'])
            key = self.ex_find_or_import_keypair_by_key_material(auth.pubkey)
            params['KeyName'] = key['keyName']

        if 'ex_keyname' in kwargs:
            params['KeyName'] = kwargs['ex_keyname']

        if 'ex_userdata' in kwargs:
            params['UserData'] = base64.b64encode(str(kwargs['ex_userdata'])) \
                .decode('utf-8')

        if 'ex_clienttoken' in kwargs:
            params['ClientToken'] = kwargs['ex_clienttoken']

        if 'ex_blockdevicemappings' in kwargs:
            params.update(self._get_block_device_mapping_params(
                kwargs['ex_blockdevicemappings']))

        if 'ex_iamprofile' in kwargs:
            if not isinstance(kwargs['ex_iamprofile'], str):
                raise AttributeError('ex_iamprofile not string')

            if kwargs['ex_iamprofile'].startswith('arn:aws:iam:'):
                params['IamInstanceProfile.Arn'] = kwargs['ex_iamprofile']
            else:
                params['IamInstanceProfile.Name'] = kwargs['ex_iamprofile']

        if 'ex_ebs_optimized' in kwargs:
            params['EbsOptimized'] = kwargs['ex_ebs_optimized']

        subnet_id = None
        if 'ex_subnet' in kwargs:
            subnet_id = kwargs['ex_subnet'].id

        if 'ex_placement_group' in kwargs and kwargs['ex_placement_group']:
            params['Placement.GroupName'] = kwargs['ex_placement_group']

        assign_public_ip = kwargs.get('ex_assign_public_ip', False)
        # In the event that a public ip is requested a NetworkInterface
        # needs to be specified.  Some properties that would
        # normally be at the root (security group ids and subnet id)
        # need to be moved to the level of the NetworkInterface because
        # the NetworkInterface is no longer created implicitly
        if assign_public_ip:
            root_key = 'NetworkInterface.1.'
            params[root_key + 'AssociatePublicIpAddress'] = "true"
            # This means that when the instance is terminated, the
            # NetworkInterface we created for the instance will be
            # deleted automatically
            params[root_key + 'DeleteOnTermination'] = "true"
            # Required to be 0 if we are associating a public ip
            params[root_key + 'DeviceIndex'] = "0"

            if subnet_id:
                params[root_key + 'SubnetId'] = subnet_id

            for key, security_group_id in security_group_id_params.items():
                key = root_key + key
                params[key] = security_group_id
        else:
            params.update(security_group_id_params)
            if subnet_id:
                params['SubnetId'] = subnet_id

        # Specify tags at instance creation time
        tags = {'Name': kwargs['name']}
        if 'ex_metadata' in kwargs:
            tags.update(kwargs['ex_metadata'])
        tagspec_root = 'TagSpecification.1.'
        params[tagspec_root + 'ResourceType'] = 'instance'
        tag_nr = 1
        for k, v in tags.items():
            tag_root = tagspec_root + 'Tag.%d.' % tag_nr
            params[tag_root + 'Key'] = k
            params[tag_root + 'Value'] = v
            tag_nr += 1

        object = self.connection.request(self.path, params=params).object
        nodes = self._to_nodes(object, 'instancesSet/item')

        for node in nodes:
            node.name = kwargs['name']
            node.extra.update({'tags': tags})

        if len(nodes) == 1:
            return nodes[0]
        else:
            return nodes
