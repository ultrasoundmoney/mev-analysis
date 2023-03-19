CREATE UNIQUE INDEX IF NOT EXISTS censorship_delay_7d_all_columns
  ON censorship_delay_7d (censored_tx_count, censored_avg_delay, uncensored_tx_count, uncensored_avg_delay);

CREATE UNIQUE INDEX IF NOT EXISTS censorship_delay_30d_all_columns
  ON censorship_delay_30d (censored_tx_count, censored_avg_delay, uncensored_tx_count, uncensored_avg_delay);
