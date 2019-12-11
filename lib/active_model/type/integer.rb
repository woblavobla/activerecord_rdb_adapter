# frozen_string_literal: true

module ActiveModel
  module Type
    class Integer < Value # :nodoc:
      include Helpers::Numeric

      private

      def _limit
        limit || 8 # 8 bytes means a bigint as opposed to smallint etc.
      end
    end
  end
end