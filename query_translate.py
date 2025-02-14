import re

def translate_to_simple_triple(query):
    triple_pattern = re.search(r'\{\s*(.*?)\s*\}', query, re.DOTALL)
    if not triple_pattern:
        return None

    triple_block = triple_pattern.group(1)
    triples = re.split(r'\s*\.\s*(?![^<]*>)', triple_block)
    if not triples:
        return None

    triple = triples[0].strip()
    parts = triple.split()

    subject, predicate, obj = '?s', '?p', '?o'

    if len(parts) == 3:
        subject, predicate, obj = parts
    elif len(parts) == 2:
        if parts[0].startswith('?'):
            subject, predicate = parts
        elif parts[1].startswith('?'):
            predicate, obj = parts
        else:
            subject, obj = parts
    elif len(parts) == 1:
        if parts[0].startswith('?'):
            subject = parts[0]
        else:
            obj = parts[0]

    subject = '?s' if subject.startswith('?') else subject
    predicate = '?p' if predicate.startswith('?') else predicate
    obj = '?o' if obj.startswith('?') else obj

    variables = ' '.join(v for v in (subject, predicate, obj) if v.startswith('?'))

    return f"SELECT {variables} WHERE {{ {subject} {predicate} {obj} . }}"

def process_sparql_file(file_path, output_path):
    with open(file_path, 'r') as file:
        queries = file.readlines()

    with open(output_path, 'w') as output_file:
        for query in queries:
            if query.strip().startswith('SELECT'):
                simple_query = translate_to_simple_triple(query)
                if simple_query:
                    output_file.write(simple_query + '\n')


# Example usage:
if __name__ == '__main__':
    file_path = 'watdiv_queries.txt'
    output_path = 'simplified_queries.txt'
    process_sparql_file(file_path, output_path)
