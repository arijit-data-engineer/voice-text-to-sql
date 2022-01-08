from flask import Flask, render_template, url_for, request, jsonify
from nltk.tokenize import word_tokenize
from flask_cors import CORS, cross_origin

app = Flask(__name__)

cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'


@app.route('/sql', methods=['POST'])
@cross_origin()
def message():

    input = request.get_json()

    # text = "Join Employee and Department Using department id to get department wise maximum salary"

    text = input['content']

    text_tokens = word_tokenize(text.lower())

    output_dict = {}

    try:

        def create_table(text_tokens):
            create_table_list = []
            create_table_list.append('create')
            create_table_list.append('table')

            for k in range(len(text_tokens)):
                if text_tokens[k] == 'table':
                    create_table_list.append(text_tokens[k+1])
                    create_table_list.append('(')
                    break

            for i in range(len(text_tokens)):
                if text_tokens[i] == 'columns':
                    for j in range(i+1, len(text_tokens)):
                        if j != len(text_tokens)-1:
                            create_table_list.append(f"{text_tokens[j]} varchar(255),")
                        else:
                            create_table_list.append(f"{text_tokens[j]} varchar(255)")
            create_table_list.append(');')
            return create_table_list

        def group_by_select(text_tokens):

            group_by_list = []
            group_by_select_list = []
            agg_dict = {"maximum": "max", "minimum": "min", "average": "avg", "mean": "avg", "total": "count", "count": "count", "sum": "sum"}

            for j in range(len(text_tokens)):
                if text_tokens[j] == 'maximum' or text_tokens[j] == 'minimum' or text_tokens[j] == 'total' or text_tokens[j] == 'average' or text_tokens[j] == 'sum' or text_tokens[j] == 'count' or text_tokens[j] == 'mean':
                    if text_tokens[j - 1] == 'wise':
                        col = text_tokens[j - 2]
                        if text_tokens[j] == 'total' or text_tokens[j] == 'sum' or text_tokens[j] == 'count':
                            agg_func = f"{agg_dict[text_tokens[j]]}(*) as aggregate_function"
                            group_by_select_list.append(col)
                            group_by_select_list.append(agg_func)
                            group_by_list.append(col)
                        else:
                            agg_func = f"{agg_dict[text_tokens[j]]}({text_tokens[j + 1]}) as aggregate_function"
                            group_by_select_list.append(col)
                            group_by_select_list.append(agg_func)
                            group_by_list.append(col)

                    else:
                        if text_tokens[j] == 'total' or text_tokens[j] == 'sum' or text_tokens[j] == 'count':
                            agg_func = f"{agg_dict[text_tokens[j]]}(*) as aggregate_function"
                            group_by_select_list.append(agg_func)
                        else:
                            agg_func = f"{agg_dict[text_tokens[j]]}({text_tokens[j + 1]}) as aggregate_function"
                            group_by_select_list.append(agg_func)

            return group_by_select_list, group_by_list

        def filter_condition(text_tokens):

            condition_dictionary = {"equal": "=", "equals": "=", "greater": ">", "less": "<", "more": ">"}
            condition_list = []
            for i in range(len(text_tokens)):
                if text_tokens[i] == 'where' or text_tokens[i] == 'when' or text_tokens[i] == 'whose':
                    condition_list.append("where")
                    for j in range(i + 1, len(text_tokens)):
                        if text_tokens[j] == 'than' or text_tokens[j] == 'of' or text_tokens[j] == 'with' or text_tokens[j] == 'off' or text_tokens[j] == 'to':
                            pass
                        else:
                            if text_tokens[j] in condition_dictionary.keys():
                                condition_list.append(condition_dictionary[text_tokens[j]])
                            elif text_tokens[j] == 'is' or text_tokens[j] == 'are':
                                if text_tokens[j+1] == 'equal' or text_tokens[j+1] == 'less' or text_tokens[j+1] == 'greater':
                                    pass
                                else:
                                    condition_list.append("=")
                            else:
                                condition_list.append(text_tokens[j])
            return condition_list

        def select_clause_with_from(text_tokens):
            current_table = ""
            select_statement_cols_list = []
            from_list = []
            for i in range(len(text_tokens)):
                if text_tokens[i] == 'get' or text_tokens[i] == 'show':
                    for l in range(i + 1, len(text_tokens)):
                        if text_tokens[l] == 'from':
                            current_table = text_tokens[l+1]
                            break
                    for j in range(i + 1, len(text_tokens)):
                        if text_tokens[j] == 'all':
                            if current_table == "":
                                current_table = text_tokens[j+1]
                            select_statement_cols_list.append(f"{current_table}.*")
                            break
                        elif text_tokens[j] == 'from':
                            break
                        elif text_tokens[j] == 'and':
                            pass
                        else:
                            select_statement_cols_list.append(f"{current_table}.{text_tokens[j]}")

                elif text_tokens[i] == 'where' or text_tokens[i] == 'when' or text_tokens[i] == 'whose':
                    break

                elif text_tokens[i] == 'and':
                    pass

                elif text_tokens[i] == 'from':
                    for k in range(i + 1, len(text_tokens)):
                        if text_tokens[k] == 'where' or text_tokens[k] == 'when' or text_tokens[k] == 'whose':
                            break
                        elif text_tokens[k] == 'and':
                            break
                        else:
                            from_list.append(text_tokens[k])
            if len(from_list) == 0:
                from_list.append(current_table)
            return select_statement_cols_list, from_list

        def joins_and_conditions(text_tokens):
            output = []
            for i in range(len(text_tokens)):

                if text_tokens[i] == 'join':
                    table_list = []
                    for j in range(i+1, len(text_tokens)):
                        if text_tokens[j] == 'using':
                            break
                        elif text_tokens[j] == 'and':
                            pass
                        else:
                            table_list.append(text_tokens[j])

                    output.append(table_list)

                elif text_tokens[i] == 'using':
                    joining_condition_list = []
                    for k in range(i+1, len(text_tokens)):
                        if text_tokens[k] == 'to' or text_tokens[k] == 'join' or text_tokens[k] == 'and':
                            break
                        else:
                            joining_condition_list.append(text_tokens[k])

                    output.append(joining_condition_list)

                elif text_tokens[i] == 'get' or text_tokens[i] == 'show':
                    break

            return output

        if text_tokens[0] == 'join':
            tables_conditions = joins_and_conditions(text_tokens)
            table_list = []
            stmt = []
            for i in range(1,len(tables_conditions),2):

                condition = "_".join(tables_conditions[i])
                table1 = tables_conditions[i-1][0]
                table2 = tables_conditions[i-1][1]
                con = f"{table1}.{condition} = {table2}.{condition}"
                if i == 1:
                    stmt.append('from')
                    stmt.append(table1)
                    stmt.append('join')
                    stmt.append(table2)
                    stmt.append('on')
                    stmt.append(con)

                else:
                    if table1 in table_list:
                        stmt.append('join')
                        stmt.append(table2)
                        stmt.append('on')
                        stmt.append(con)
                    else:
                        stmt.append('join')
                        stmt.append(table1)
                        stmt.append('on')
                        stmt.append(con)

                table_list.append(table1)
                table_list.append(table2)

            join_stmt = " ".join(stmt)
            if 'maximum' in text_tokens or 'minimum' in text_tokens or 'total' in text_tokens or 'average' in text_tokens or 'sum' in text_tokens or 'count' in text_tokens or 'mean' in text_tokens:

                group_by_select_list, group_by_list = group_by_select(text_tokens)
                select_stmt = ",".join(group_by_select_list)
                group_by_stmt = ",".join(group_by_list)
                filter_condition_list = filter_condition(text_tokens)
                filter_stmt = " ".join(filter_condition_list)
                if len(group_by_list) > 0:
                    sql_statement = f"select {select_stmt} {join_stmt} {filter_stmt} group by {group_by_stmt}"
                    output_dict["sql_statement"] = sql_statement
                    return output_dict
                else:
                    sql_statement = f"select {select_stmt} {join_stmt} {filter_stmt}"
                    output_dict["sql_statement"] = sql_statement
                    return output_dict
            else:

                select_list, from_table_list = select_clause_with_from(text_tokens)
                select_stmt = ",".join(select_list)
                filter_condition_list = filter_condition(text_tokens)
                filter_stmt = " ".join(filter_condition_list)
                sql_statement = f"select {select_stmt} {join_stmt} {filter_stmt}"
                output_dict["sql_statement"] = sql_statement
                return output_dict

        elif text_tokens[0] == 'create' and text_tokens[1] == 'table':
            create_table_list = create_table(text_tokens)
            create_table_stmt = " ".join(create_table_list)
            output_dict["sql_statement"] = create_table_stmt
            return output_dict

        else:
            if 'maximum' in text_tokens or 'count' in text_tokens or 'minimum' in text_tokens or 'total' in text_tokens or 'average' in text_tokens or 'sum' in text_tokens or 'count' in text_tokens or 'mean' in text_tokens:

                select_list, from_table_list = select_clause_with_from(text_tokens)
                group_by_select_list, group_by_list = group_by_select(text_tokens)
                filter_condition_list = filter_condition(text_tokens)
                select_stmt = ",".join(group_by_select_list)
                from_stmt = " ".join(from_table_list)
                filter_stmt = " ".join(filter_condition_list)
                group_by_stmt = ",".join(group_by_list)
                if len(group_by_list) > 0:
                    sql_statement = f"select {select_stmt} from {from_stmt} {filter_stmt} group by {group_by_stmt}"
                    output_dict["sql_statement"] = sql_statement
                    return output_dict
                else:
                    sql_statement = f"select {select_stmt} from {from_stmt} {filter_stmt}"
                    output_dict["sql_statement"] = sql_statement
                    return output_dict

            else:

                select_list, from_table_list = select_clause_with_from(text_tokens)
                filter_condition_list = filter_condition(text_tokens)
                select_stmt = ",".join(select_list)
                from_stmt = " ".join(from_table_list)
                filter_stmt = " ".join(filter_condition_list)
                sql_statement = f"select {select_stmt} from {from_stmt} {filter_stmt}"
                output_dict["sql_statement"] = sql_statement
                return output_dict

    except:
        print("Kindly provide a valid entry")
        output_dict["sql_statement"] = "Kindly provide a valid entry"
        return output_dict


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=4042, debug=True)