module Arel
  module Visitors
    class Rdb < Arel::Visitors::ToSql # :nodoc
      private

      def visit_Arel_Nodes_SelectStatement o, collector
        collector << "SELECT "
        collector = visit o.offset, collector if o.offset && !o.limit

        collector = o.cores.inject(collector) {|c, x|
          visit_Arel_Nodes_SelectCore(x, c)
        }

        unless o.orders.empty?
          collector << ORDER_BY
          len = o.orders.length - 1
          o.orders.each_with_index {|x, i|
            collector = visit(x, collector)
            collector << COMMA unless len == i
          }
        end

        if o.limit && o.offset
          collector = limit_with_rows o, collector
        elsif o.limit && !o.offset
          collector = visit o.limit, collector
        end

        maybe_visit o.lock, collector
      end

      def visit_Arel_Nodes_SelectCore o, collector
        if o.set_quantifier
          collector = visit o.set_quantifier, collector
          collector << SPACE
        end

        unless o.projections.empty?
          len = o.projections.length - 1
          o.projections.each_with_index do |x, i|
            collector = visit(x, collector)
            collector << COMMA unless len == i
          end
        end

        if o.source && !o.source.empty?
          collector << " FROM "
          collector = visit o.source, collector
        end

        unless o.wheres.empty?
          collector << WHERE
          len = o.wheres.length - 1
          o.wheres.each_with_index do |x, i|
            collector = visit(x, collector)
            collector << AND unless len == i
          end
        end
        unless o.groups.empty?
          collector << GROUP_BY
          len = o.groups.length - 1
          o.groups.each_with_index do |x, i|
            collector = visit(x, collector)
            collector << COMMA unless len == i
          end
        end

        if Rails::VERSION::MAJOR < 5
          collector = maybe_visit o.having, collector
        else
          unless o.havings.empty?
            collector << " HAVING "
            inject_join o.havings, collector, AND
          end
        end

        collector
      end

      def visit_Arel_Nodes_Limit o, collector
        collector << " ROWS "
        visit o.expr, collector
      end

      def visit_Arel_Nodes_Offset o, collector
        collector << " SKIP "
        visit o.expr, collector
      end

      def visit_Arel_Nodes_BindParam o, collector
        if collector.is_a?(Arel::Collectors::SubstituteBinds)
          collector.send('delegate').add_bind(o.value) { '?' }
        else
          collector.add_bind(o.value) { '?' }
        end
      end

      def limit_with_rows o, collector
        o.offset.expr.value = ActiveModel::Attribute.with_cast_value("OFFSET".freeze,
                                                                     o.offset.expr.value.value + 1,
                                                                     ActiveModel::Type.default_value)
        offset = o.offset.expr.value
        o.limit.expr.value = ActiveModel::Attribute.with_cast_value("LIMIT".freeze,
                                                                    (o.limit.expr.value.value) + (offset.value - 1),
                                                                    ActiveModel::Type.default_value)
        limit = o.limit.expr.value
        collector << " ROWS "
        collector.add_bind(offset) {|i| "?"}
        collector << " TO "
        collector.add_bind(limit) {|i| "?"}
      end

      def quote_column_name name
        return name if Arel::Nodes::SqlLiteral === name
        @connection.quote_column_name(name)
      end

    end
  end
end
