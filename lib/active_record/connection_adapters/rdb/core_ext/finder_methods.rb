require 'active_record/relation'
require 'active_record/relation/finder_methods'

ActiveRecord::Relation.class_eval do
  prepend(FinderMethodsExtensions = Module.new do
    def exists?(conditions = :none)
      if ActiveRecord::Base === conditions
        raise ArgumentError, <<-MSG.squish
          You are passing an instance of ActiveRecord::Base to `exists?`.
          Please pass the id of the object by calling `.id`.
        MSG
      end

      return false if !conditions || limit_value == 0

      if eager_loading?
        relation = apply_join_dependency(eager_loading: false)
        return relation.exists?(conditions)
      end

      relation = construct_relation_for_exists(conditions)

      skip_query_cache_if_necessary { connection.select_value(relation, "#{name} Exists") } ? true : false
    rescue ::RangeError
      false
    end
  end)
end