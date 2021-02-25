from milvus import Milvus

RED = '\033[0;31m'
GREEN = '\033[0;32m'
ENDC = ''


def test(host='127.0.0.1', port=19531):
    try:
        client = Milvus(host=host, port=port)
        print('{}Pass: Connected{}'.format(GREEN, ENDC))
        return 0
    except Exception as exc:
        print('{}Error: {}{}'.format(RED, exc, ENDC))
        return 1


if __name__ == '__main__':
    import fire
    fire.Fire(test)
