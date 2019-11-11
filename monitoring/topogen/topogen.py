import yaml

def process_type(type):
    print(type)


file = open('tosca_sample.yml')
spec = yaml.safe_load_all(file)

nodes = {}
for docs in spec:
    for key, values in docs.items():
        if key == 'node_templates':
            nodes.update(dict(values))
            break

for node in nodes:
    props = dict(nodes.get(node))
    process_type(props.get("type"))

#for data in spec:
#    for k, v in data.items():
#        print(k, "->", v)


