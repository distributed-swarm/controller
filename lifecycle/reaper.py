from lifecycle.reaper import AgentReaper

reaper = AgentReaper(store, publish_event, delete_agent, logger)
reaper.start()
