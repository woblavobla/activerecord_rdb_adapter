require 'active_record/relation/query_methods'

ActiveRecord::Relation.class_eval do
  prepend(QueryMethodExtensions = Module.new do
    def build_arel(aliases)
      arel = Arel::SelectManager.new(table)

      aliases = build_joins(arel, joins_values.flatten, aliases) unless joins_values.empty?
      build_left_outer_joins(arel, left_outer_joins_values.flatten, aliases) unless left_outer_joins_values.empty?

      arel.where(where_clause.ast) unless where_clause.empty?
      arel.having(having_clause.ast) unless having_clause.empty?
      if limit_value
        limit_attribute = ActiveModel::Attribute.with_cast_value(
            "LIMIT".freeze,
            connection.sanitize_limit(limit_value),
            ActiveModel::Type.default_value,
            )
        arel.take(Arel::Nodes::BindParam.new(limit_attribute))
      end
      if offset_value
        offset_attribute = ActiveModel::Attribute.with_cast_value(
            "OFFSET".freeze,
            offset_value.to_i,
            ActiveModel::Type.default_value,
            )
        arel.skip(Arel::Nodes::BindParam.new(offset_attribute))
      end
      arel.group(*arel_columns(group_values.uniq.reject(&:blank?))) unless group_values.empty?

      build_order(arel)

      build_select(arel)

      arel.distinct(distinct_value)
      arel.from(build_from) unless from_clause.empty?
      arel.lock(lock_value) if lock_value

      arel
    end
  end)
end