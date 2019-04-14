import sys
import yaml
import hashlib

DEFINE = 'YAML_GEN_TESTS'
EVENT_COUNT = 5

def encode_stream(line):
    for c in line:
        if c == '\n':
            yield '\\n'
        elif c == '"':
            yield '\\"'
        elif c == '\t':
            yield '\\t'
        elif ord(c) < 0x20:
            yield '\\x' + hex(ord(c))
        else:
            yield c

def encode(line):
    return ''.join(encode_stream(line))

def doc_start(implicit=False):
    if implicit:
        return {'emit': '', 'handle': 'OnDocumentStart(_)'}
    else:
        return {'emit': 'BeginDoc', 'handle': 'OnDocumentStart(_)'}

def doc_end(implicit=False):
    if implicit:
        return {'emit': '', 'handle': 'OnDocumentEnd()'}
    else:
        return {'emit': 'EndDoc', 'handle': 'OnDocumentEnd()'}

def scalar(value, tag='', anchor='', anchor_id=0):
    emit = []
    if tag:
        emit += ['VerbatimTag("%s")' % encode(tag)]
    if anchor:
        emit += ['Anchor("%s")' % encode(anchor)]
    if tag:
        out_tag = encode(tag)
    else:
        if value == encode(value):
            out_tag = '?'
        else:
            out_tag = '!'
    emit += ['"%s"' % encode(value)]
    return {'emit': emit, 'handle': 'OnScalar(_, "%s", %s, "%s")' % (out_tag, anchor_id, encode(value))}

def comment(value):
    return {'emit': 'Comment("%s")' % value, 'handle': ''}

def seq_start(tag='', anchor='', anchor_id=0, style='_'):
    emit = []
    if tag:
        emit += ['VerbatimTag("%s")' % encode(tag)]
    if anchor:
        emit += ['Anchor("%s")' % encode(anchor)]
    if tag:
        out_tag = encode(tag)
    else:
        out_tag = '?'
    emit += ['BeginSeq']
    return {'emit': emit, 'handle': 'OnSequenceStart(_, "%s", %s, %s)' % (out_tag, anchor_id, style)}

def seq_end():
    return {'emit': 'EndSeq', 'handle': 'OnSequenceEnd()'}

def map_start(tag='', anchor='', anchor_id=0, style='_'):
    emit = []
    if tag:
        emit += ['VerbatimTag("%s")' % encode(tag)]
    if anchor:
        emit += ['Anchor("%s")' % encode(anchor)]
    if tag:
        out_tag = encode(tag)
    else:
        out_tag = '?'
    emit += ['BeginMap']
    return {'emit': emit, 'handle': 'OnMapStart(_, "%s", %s, %s)' % (out_tag, anchor_id, style)}

def map_end():
    return {'emit': 'EndMap', 'handle': 'OnMapEnd()'}

def gen_templates():
    yield [[doc_start(), doc_start(True)],
           [scalar('foo'), scalar('foo\n'), scalar('foo', 'tag'), scalar('foo', '', 'anchor', 1)],
           [doc_end(), doc_end(True)]]
    yield [[doc_start(), doc_start(True)],
           [seq_start()],
           [[], [scalar('foo')], [scalar('foo', 'tag')], [scalar('foo', '', 'anchor', 1)], [scalar('foo', 'tag', 'anchor', 1)], [scalar('foo'), scalar('bar')], [scalar('foo', 'tag', 'anchor', 1), scalar('bar', 'tag', 'other', 2)]],
           [seq_end()],
           [doc_end(), doc_end(True)]]
    yield [[doc_start(), doc_start(True)],
           [map_start()],
           [[], [scalar('foo'), scalar('bar')], [scalar('foo', 'tag', 'anchor', 1), scalar('bar', 'tag', 'other', 2)]],
           [map_end()],
           [doc_end(), doc_end(True)]]
    yield [[doc_start(True)],
           [map_start()],
           [[scalar('foo')], [seq_start(), scalar('foo'), seq_end()], [map_start(), scalar('foo'), scalar('bar'), map_end()]],
           [[scalar('foo')], [seq_start(), scalar('foo'), seq_end()], [map_start(), scalar('foo'), scalar('bar'), map_end()]],
           [map_end()],
           [doc_end(True)]]
    yield [[doc_start(True)],
           [seq_start()],
           [[scalar('foo')], [seq_start(), scalar('foo'), seq_end()], [map_start(), scalar('foo'), scalar('bar'), map_end()]],
           [[scalar('foo')], [seq_start(), scalar('foo'), seq_end()], [map_start(), scalar('foo'), scalar('bar'), map_end()]],
           [seq_end()],
           [doc_end(True)]]

def expand(template):
    if len(template) == 0:
        pass
    elif len(template) == 1:
        for item in template[0]:
            if isinstance(item, list):
                yield item
            else:
                yield [item]
    else:
        for car in expand(template[:1]):
            for cdr in expand(template[1:]):
                yield car + cdr
            

def gen_events():
    for template in gen_templates():
        for events in expand(template):
            base = list(events)
            for i in range(0, len(base)+1):
                cpy = list(base)
                cpy.insert(i, comment('comment'))
                yield cpy

def gen_tests():
    for events in gen_events():
        name = 'test' + hashlib.sha1(''.join(yaml.dump(event) for event in events)).hexdigest()[:20]
        yield {'name': name, 'events': events}

class Writer(object):
    def __init__(self, out):
        self.out = out
        self.indent = 0
    
    def writeln(self, s):
        self.out.write('%s%s\n' % (' ' * self.indent, s))

class Scope(object):
    def __init__(self, writer, name, indent):
        self.writer = writer
        self.name = name
        self.indent = indent

    def __enter__(self):
        self.writer.writeln('%s {' % self.name)
        self.writer.indent += self.indent
    
    def __exit__(self, type, value, traceback):
        self.writer.indent -= self.indent
        self.writer.writeln('}')

def create_emitter_tests(out):
    out = Writer(out)
    
    includes = [
        'handler_test.h',
        'yaml-cpp/yaml.h',
        'gmock/gmock.h',
        'gtest/gtest.h',
    ]
    for include in includes:
        out.writeln('#include "%s"' % include)
    out.writeln('')

    usings = [
        '::testing::_',
    ]
    for using in usings:
        out.writeln('using %s;' % using)
    out.writeln('')

    with Scope(out, 'namespace YAML', 0) as _:
        with Scope(out, 'namespace', 0) as _:
            out.writeln('')
            out.writeln('typedef HandlerTest GenEmitterTest;')
            out.writeln('')
            tests = list(gen_tests())

            for test in tests:
                with Scope(out, 'TEST_F(%s, %s)' % ('GenEmitterTest', test['name']), 2) as _:
                    out.writeln('Emitter out;')
                    for event in test['events']:
                        emit = event['emit']
                        if isinstance(emit, list):
                            for e in emit:
                                out.writeln('out << %s;' % e)
                        elif emit:
                            out.writeln('out << %s;' % emit)
                    out.writeln('')
                    for event in test['events']:
                        handle = event['handle']
                        if handle:
                            out.writeln('EXPECT_CALL(handler, %s);' % handle)
                    out.writeln('Parse(out.c_str());')
                out.writeln('')

if __name__ == '__main__':
    create_emitter_tests(sys.stdout)
