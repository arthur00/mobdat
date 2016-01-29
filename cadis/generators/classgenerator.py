from cadis.language.schema import dimension, Set, SubSet, CADIS, dimensions, sets, subsets, primarykey
import json, sys

def deunicodify_hook(pairs):
    new_pairs = []
    for key, value in pairs:
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        new_pairs.append((key, value))
    return dict(new_pairs)

def generate(JSON, attachTo=None):
    definitions = json.loads(JSON, object_pairs_hook=deunicodify_hook)

    classes = {}
    for cls_name, cls_def in definitions.iteritems():
        attrs = {}
        for attr, defs in cls_def['attributes'].iteritems():
            var = '_' + attr

            def prop(self):
                return getattr(self, var)

            def setter(self, value):
                return setattr(self, var, value)

            if 'default' in defs:
                attrs[var] = defs['default']
            else:
                attrs[var] = None

            prop = dimension(prop)
            prop = prop.setter(setter)

            attrs[attr] = prop

        cls = type(cls_name, (CADIS, ), attrs)
        
        if 'set' in cls_def and cls_def['set']:
            cls = Set(cls)

        if attachTo is not None:
            # Attach the class name to module or class attributes. Might be useful
            module = sys.modules[attachTo]
            setattr(module, cls_name, cls)

        classes[cls_name] = cls
    return classes

if __name__ == '__main__':
    NewClass = generate('''{ "PositionObject": {
      "attributes": {
        "Position": {"type": "Vector3"}
       },
       "set": true
        }
    }
    ''')["PositionObject"]
    no = NewClass()
    print no.ID
    print no.Position
    no.Position = 10
    print no.Position
    print dimensions
    print sets
    print subsets



