#--
# Copyright 2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License").
# You may not use this file except in compliance with the License.
# A copy of the License is located at
#
#  http://aws.amazon.com/apache2.0
#
# or in the "license" file accompanying this file. This file is distributed
# on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
# express or implied. See the License for the specific language governing
# permissions and limitations under the License.
#++

module AWS
  module Flow


    # A decision task handler to work with a {WorkflowTaskPoller}. Create a
    # `DecisionTaskHandler` and pass it to {WorkflowTaskPoller} on
    # {WorkflowTaskPoller#initialize construction}.
    class DecisionTaskHandler

      class << self
        # Factory method to create a DecisionTaskHandler instance given a
        # workflow class
        def from_workflow_class workflow_class
          self.new(WorkflowDefinitionFactory.generate_definition_map(workflow_class))
        end
      end

      attr_reader :workflow_definition_map

      # Creates a new `DecisionTaskHandler`.
      #
      # @param workflow_definition_map
      #
      # @param options
      #   An optional logger.
      #
      def initialize(workflow_definition_map, options=nil)
        @workflow_definition_map = workflow_definition_map
        @logger = options.logger if options
        @logger ||= Utilities::LogFactory.make_logger(self)
      end


      # Handles a decision task.
      #
      # @param decision_task_iterator
      #
      def handle_decision_task(decision_task_iterator)
        history_helper = HistoryHelper.new(decision_task_iterator)
        @logger.debug "history helper: #{history_helper.inspect}"
        decider = create_async_decider(history_helper)
        @logger.debug "decider: #{decider.inspect}"
        decider.decide
        decisions = decider.get_decisions
        @logger.debug "decisions: #{decisions.inspect}"
        response = {:task_token => decider.task_token}
        @logger.debug "response: #{response.inspect}"
        context_data = decider.decision_helper.workflow_context_data
        @logger.debug "context_data: #{context_data.inspect}"
        response[:execution_context] = context_data.to_s unless context_data.nil?
        response[:decisions] = decisions unless decisions.nil?
        @logger.debug "response========: #{response.inspect}"
        return response
      end

      # Creates a new asynchronous decider.
      #
      # @param history_helper
      #
      # @return [AsyncDecider] The created {AsyncDecider}.
      #
      def create_async_decider(history_helper)
        @logger.debug "create_async_decider: ============"
        task = history_helper.get_decision_task
        @logger.debug "task: ============#{task.inspect}"

        workflow_type = task.workflow_type
        @logger.debug "workflow_type: ============#{workflow_type.inspect}"

        # TODO put in context correctly
        @logger.debug "workflow_definition_map: ============#{@workflow_definition_map.inspect}"
        workflow_definition_factory = @workflow_definition_map[workflow_type]
        @logger.debug "workflow_definition_map: ============#{workflow_definition_factory.inspect}"
        raise "No workflow definition for #{workflow_type.inspect}" if workflow_definition_factory.nil?
        AsyncDecider.new(workflow_definition_factory, history_helper, DecisionHelper.new)
      end

    end

  end
end
