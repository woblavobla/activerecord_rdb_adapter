require 'active_record/relation'

ActiveRecord::Relation.class_eval do
  prepend(RelationExtensions = Module.new do
    def update_all(updates)
      raise ArgumentError, "Empty list of attributes to change" if updates.blank?

      stmt = Arel::UpdateManager.new

      stmt.table(table)
      value_binds = []
      arel_values = []
      updates.each do |column, value|
        value_binds.push ActiveRecord::Relation::QueryAttribute.new(column, value, @klass.type_for_attribute(column))
        arel_values.push Arel::Nodes::Assignment.new(Arel::Nodes::UnqualifiedColumn.new(Arel::Attributes::Attribute.new(table, column)), Arel::Nodes::BindParam.new)
      end
      stmt.ast.values = arel_values

      if has_join_values?
        @klass.connection.join_to_update(stmt, arel, arel_attribute(primary_key))
      else
        stmt.key = arel_attribute(primary_key)
        stmt.take(arel.limit)
        stmt.order(*arel.orders)
        stmt.wheres = arel.constraints
      end

      @klass.connection.update stmt, "SQL", value_binds + bound_attributes
    end
  end)
end