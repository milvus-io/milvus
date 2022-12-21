from utils.util_log import test_log as log


def ip_check(ip):
    if ip == "localhost":
        return True

    if not isinstance(ip, str):
        log.error("[IP_CHECK] IP(%s) is not a string." % ip)
        return False

    return True


def number_check(num):
    if str(num).isdigit():
        return True

    else:
        log.error("[NUMBER_CHECK] Number(%s) is not a numbers." % num)
        return False
