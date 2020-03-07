import logging

logger = logging.getLogger(__name__)


class TopoObject:
    def __init__(self, name, **kwargs):
        self.name = name
        self.kwargs = kwargs

    def __eq__(self, other):
        if isinstance(other, str):
            return self.name == other
        return self.name == other.name

    def __hash__(self):
        return hash(self.name)

    def __str__(self):
        return '<TopoObject: {}>'.format(self.name)


class TopoGroup:
    def __init__(self, name):
        self.name = name
        self.items = set()

    def _add(self, topo_object):
        if topo_object in self.items:
            logger.warning('Duplicated topo_object \"{}\" into group \"{}\"'.format(topo_object, self.name))
            return False
        logger.warning('Adding topo_object \"{}\" into group \"{}\"'.format(topo_object, self.name))
        self.items.add(topo_object)

    def add(self, topo_object):
        if isinstance(topo_object, str):
            return self._add(TopoObject(topo_object))
        else:
            return self._add(topo_object)

    def __len__(self):
        return len(self.items)

    def __str__(self):
        return '<TopoGroup: {}>'.format(self.name)


class Topology:
    def __init__(self):
        self.topo_groups = {}

    def on_duplicated_group(self, group):
        logger.warning('Duplicated group \"{}\" found!'.format(group))

    def on_pre_add_group(self, group):
        logger.debug('Pre add group \"{}\"'.format(group))

    def on_post_add_group(self, group):
        logger.debug('Post add group \"{}\"'.format(group))

    def has_group(self, group):
        key = group if isinstance(group, str) else group
        return key in self.topo_groups

    def add_group(self, group):
        self.on_pre_add_group(group)
        if self.has_group(group):
            return on_duplicated_group(group)
        logger.info('Adding group \"{}\"'.format(group))
        self.topo_groups[group.name] = group
        return self.on_post_add_group(group)

    def on_delete_not_existed_group(self, group):
        logger.warning('Deleting non-existed group \"{}\"'.format(group))

    def on_pre_delete_group(self, group):
        logger.debug('Pre delete group \"{}\"'.format(group))

    def on_post_delete_group(self, group):
        logger.debug('Post delete group \"{}\"'.format(group))

    def _delete_group(self, group):
        delete_key = group if isinstance(group, str) else group.name
        return self.topo_groups.pop(delete_key, None)

    def delete_group(self, group):
        self.on_pre_delete_group(group)
        logger.info('Deleting group \"{}\"'.format(group))
        deleted_group = self._delete_group(group)
        if not deleted_group:
            return self.on_delete_not_existed_group(group)
        return self.on_post_delete_group(group)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    o1 = TopoObject('x1')
    o2 = TopoObject('x1')
    print(o1==o2)

    s = set()
    print(s.add(o1))
    print(len(s))
    s.add(o1)
    print(len(s))
    s.add(o2)
    print(len(s))
    n = 'x1'
    print(s.add(n))
    print(len(s))
    print(n in s)

    group = TopoGroup('g1')
    print(len(group))

    group.add(o1)
    print(len(group))
    group.add(o2)
    print(len(group))

    topo = Topology()
    topo.add_group(group)
    topo.delete_group('x2')

    topo.delete_group('g1')
