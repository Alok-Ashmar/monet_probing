from langchain_core.messages import SystemMessage

class RelevanceChecker:
    @staticmethod
    def check_and_update_prompt(probe, metric):
        """
        Check for relevance threshold and add a relevance prompt if needed.
        """
        # Extract relevance score from metric
        if isinstance(metric, dict):
            relevance = metric.get("relevance", 0)
        else:
            relevance = getattr(metric, "relevance", 0)
            
        if relevance < probe.relevance_threshold:
            RelevanceChecker.add_relevance_prompt(probe)

    @staticmethod
    def add_relevance_prompt(probe):
        """
        Append relevance prompt to the system prompt if not already added.
        """
        if probe.relevance_prompt_added:
            return
        
        # Append relevance-chk to __system_prompt__
        probe.__system_prompt__ += f"\n {probe.__prompt_chunks__.get('relevance-chk', '')}"
        probe.relevance_prompt_added = True

        # Update the system message in the conversation history
        messages = probe._history.messages
        if messages and isinstance(messages[0], SystemMessage):
            # The first message is the SystemMessage
            messages[0] = SystemMessage(content=probe.__system_prompt__)
            probe._history.clear()
            for msg in messages:
                probe._history.add_message(msg)
