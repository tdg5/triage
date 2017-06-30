class ModelSelector(object):
    def __init__(self, db_engine, models_table):
        self.db_engine = db_engine
        self.models_table = models_table
        

    def _create_table(self, table_name):
        self.db_engine.execute('''create table {} (
            model_group_id int,
            model_id int,
            train_end_time timestamp,
            metric text,
            parameter text,
            below_max float,
            below_max_next_time float
        )'''.format(table_name))

    def _populate_table(self, model_group_ids, metrics, output_table):
        for metric in metrics:
            self.db_engine.execute('''
                insert into {new_table}
                WITH model_ranks AS (
                  SELECT m.model_group_id, m.model_id, m.train_end_time, ev.value,
                         row_number() OVER (PARTITION BY m.train_end_time ORDER BY ev.value DESC, RANDOM()) AS rank
                  FROM results.evaluations ev
                  JOIN results.{models_table} m USING(model_id)
                  JOIN results.model_groups mg USING(model_group_id)
                  WHERE ev.metric='{metric}' AND ev.parameter='{metric_param}'
                        AND m.model_group_id IN ({model_group_ids})
                ),
                model_tols AS (
                  SELECT train_end_time, model_group_id, model_id,
                         rank,
                         value,
                         first_value(value) over (partition by train_end_time order by rank ASC) AS best_val
                  FROM model_ranks
                ),
                current_best_vals as (
                    SELECT model_group_id, model_id, train_end_time, '{metric}', '{metric_param}', best_val - value below_max
                    FROM model_tols
                )
                select current_best_vals.*, first_value(below_max) over (partition by model_group_id order by train_end_time asc rows between 1 following and unbounded following) below_max_next_time
                from current_best_vals
            '''.format(
                model_group_ids=','.join(map(str, model_group_ids)),
                models_table=self.models_table,
                metric=metric['metric'],
                metric_param=metric['param'],
                new_table=output_table
            ))

    def create_and_populate_table(self, model_group_ids, metrics, output_table):
        self._create_table(output_table)
        self._populate_table(model_group_ids, metrics, output_table)

    def phase_one(self, model_group_ids, metrics, output_table):
        for metric in metrics:
            query = '''
                select 
            '''
The x-axis is the same value of X (difference from the best performing) and the y-axis is the fraction of models in that model group whose metric values are within X of the best model evaluated on the same test set, with each line being a model group. That is, for each test set, calculate the metric on all of the models, find the best one, and calculate the distance from the best for the others, then aggregate to the model group level asking for each value of X what fraction of models in that group were that close to the best one. 
