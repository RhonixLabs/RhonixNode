class ProposerSpec {

  // Propose should result in block creation

  // Repeated call for propose while another propose is in progress should not trigger proposal

  // Proposer should expand the view with attestations added during message creation if final fringe IS NOT changed

  // Proposer should NOT expand the view with attestations added during message creation if final fringe IS changed

  // Proposer should NOT expand the view with state transitions added during message creation

  // Mixed scenario - attestations and state transitions. Expansion should be done correctly.
}
