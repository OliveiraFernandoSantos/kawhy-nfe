package br.com.actionsys.kawhynfe;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
@Slf4j
public class Schedule {
  @Autowired
  private Orchestrator orchestrator;

  @Scheduled(fixedDelayString = "${schedule.delay-seconds}", timeUnit = TimeUnit.SECONDS)
  public void run() {
    orchestrator.process();
  }

}
