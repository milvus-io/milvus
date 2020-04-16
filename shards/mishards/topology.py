import logging
import threading
import enum

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

class StatusType(enum.Enum):
    OK = 1
    DUPLICATED = 2
    ADD_ERROR = 3
    VERSION_ERROR = 4


class TopoGroup:
    def __init__(self, name):
        self.name = name
        self.items = {}
        self.cv = threading.Condition()

    def on_duplicate(self, topo_object):
        pass
        # logger.warning('Duplicated topo_object \"{}\" into group \"{}\"'.format(topo_object, self.name))

    def on_added(self, topo_object):
        return True

    def on_pre_add(self, topo_object):
        return True

    def _add_no_lock(self, topo_object):
        if topo_object.name in self.items:
            return StatusType.DUPLICATED
        logger.info('Adding topo_object \"{}\" into group \"{}\"'.format(topo_object, self.name))
        ok = self.on_pre_add(topo_object)
        if not ok:
            return StatusType.VERSION_ERROR
        self.items[topo_object.name] = topo_object
        ok = self.on_added(topo_object)
        if not ok:
            self._remove_no_lock(topo_object.name)

        return StatusType.OK if ok else StatusType.ADD_ERROR

    def add(self, topo_object):
        with self.cv:
            return self._add_no_lock(topo_object)

    def __len__(self):
        return len(self.items)

    def __str__(self):
        return '<TopoGroup: {}>'.format(self.name)

    def get(self, name):
        return self.items.get(name, None)

    def _remove_no_lock(self, name):
        logger.info('Removing topo_object \"{}\" from group \"{}\"'.format(name, self.name))
        return self.items.pop(name, None)

    def remove(self, name):
        with self.cv:
            return self._remove_no_lock(name)


class Topology:
    def __init__(self):
        self.topo_groups = {}
        self.cv = threading.Condition()

    def on_duplicated_group(self, group):
        # logger.warning('Duplicated group \"{}\" found!'.format(group))
        return StatusType.DUPLICATED

    def on_pre_add_group(self, group):
        # logger.debug('Pre add group \"{}\"'.format(group))
        return StatusType.OK

    def on_post_add_group(self, group):
        # logger.debug('Post add group \"{}\"'.format(group))
        return StatusType.OK

    def get_group(self, name):
        return self.topo_groups.get(name, None)

    def has_group(self, group):
        key = group if isinstance(group, str) else group.name
        return key in self.topo_groups

    def _add_group_no_lock(self, group):
        logger.info('Adding group \"{}\"'.format(group))
        self.topo_groups[group.name] = group

    def add_group(self, group):
        self.on_pre_add_group(group)
        if self.has_group(group):
            return self.on_duplicated_group(group)
        with self.cv:
            self._add_group_no_lock(group)
        return self.on_post_add_group(group)

    def on_delete_not_existed_group(self, group):
        # logger.warning('Deleting non-existed group \"{}\"'.format(group))
        pass

    def on_pre_delete_group(self, group):
        pass
        # logger.debug('Pre delete group \"{}\"'.format(group))

    def on_post_delete_group(self, group):
        pass
        # logger.debug('Post delete group \"{}\"'.format(group))

    def _delete_group_no_lock(self, group):
        logger.info('Deleting group \"{}\"'.format(group))
        delete_key = group if isinstance(group, str) else group.name
        return self.topo_groups.pop(delete_key, None)

    def delete_group(self, group):
        self.on_pre_delete_group(group)
        with self.cv:
            deleted_group = self._delete_group_no_lock(group)
        if not deleted_group:
            return self.on_delete_not_existed_group(group)
        return self.on_post_delete_group(group)

    @property
    def group_names(self):
        return self.topo_groups.keys()
