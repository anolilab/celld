//! The production reader decision against the model's, over every combination
//! of a small domain.

use crate::lazy_birth::{self as model, Design};
use crate::session::LeftEntry;
use crate::session_take as prod;

type LeftCase = Option<(u8, Vec<u8>, Option<u8>)>;

const MINTER: u8 = 7;
const OTHER: u8 = 5;

fn name(nonce: u8) -> String {
    format!("n{nonce}")
}

fn mode_pair(index: usize) -> (model::Mode, prod::Mode) {
    [
        (model::Mode::Shadow, prod::Mode::Shadow),
        (model::Mode::Read, prod::Mode::Read),
        (model::Mode::On, prod::Mode::On),
    ][index]
}

fn same(model_take: model::Take, prod_take: prod::Take) -> bool {
    use model::Take as M;
    use prod::Take as P;
    match (model_take, prod_take) {
        (M::Refuse(a), P::Refuse(b)) => a == b,
        (M::Standard { epoch: a }, P::Standard { epoch: b })
        | (M::FirstFromNothing { epoch: a }, P::FirstFromNothing { epoch: b })
        | (M::FirstNew { epoch: a }, P::FirstNew { epoch: b })
        | (M::FirstOverRecord { epoch: a }, P::FirstOverRecord { epoch: b })
        | (M::Plain { epoch: a }, P::Plain { epoch: b })
        | (M::ReplaceFirst { epoch: a }, P::ReplaceFirst { epoch: b })
        | (M::ValidTakeover { epoch: a }, P::ValidTakeover { epoch: b }) => u64::from(a) == b,
        (M::Forward, P::Forward) => true,
        _ => false,
    }
}

#[test]
fn the_reader_decision_matches_the_model_over_the_whole_domain() {
    let design = Design::rev4();
    let nonces: [Option<u8>; 3] = [None, Some(1), Some(2)];
    let lefts: [LeftCase; 4] = [
        None,
        Some((1, vec![1], None)),
        Some((1, vec![1], Some(1))),
        Some((2, vec![1, 2], None)),
    ];
    let mut cases = 0_u32;
    for mode in 0..3 {
        for hdr_state in 0..3 {
            // 0: no header, 1: a lost header, 2: a header.
            for chunk_state in 0..3 {
                // 0: no chunk, 1: an unknown chunk, 2: a chunk.
                for granted in [false, true] {
                    for taken in nonces {
                        for left in &lefts {
                            for rec_state in 0..6 {
                                // 0: none, 1: plain by the minter at epoch 1, 2: plain
                                // by another, 3: first-form, 4: leave-form, 5: released plain.
                                for author_live in [false, true] {
                                    for over in [vec![], vec![1_u8], vec![1, 2, 3, 4]] {
                                        cases += check(
                                            &design,
                                            mode,
                                            hdr_state,
                                            chunk_state,
                                            granted,
                                            taken,
                                            left,
                                            rec_state,
                                            author_live,
                                            over,
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    assert!(cases > 10_000, "{cases}");
}

#[allow(clippy::too_many_arguments)]
fn check(
    design: &Design,
    mode: usize,
    hdr_state: usize,
    chunk_state: usize,
    granted: bool,
    taken: Option<u8>,
    left: &LeftCase,
    rec_state: usize,
    author_live: bool,
    over: Vec<u8>,
) -> u32 {
    let (model_mode, prod_mode) = mode_pair(mode);
    let m_hdr = (hdr_state > 0).then_some(model::Hdr {
        mode: model_mode,
        state: model::HState::Sealed,
        chunks: 1,
        lost: hdr_state == 1,
        rev: 0,
        token: 0,
    });
    let p_hdr = (hdr_state > 0).then_some(prod::Hdr {
        mode: prod_mode,
        lost: hdr_state == 1,
    });
    let m_chunk = (chunk_state > 0).then(|| {
        let mut chunk = model::Chunk::open();
        chunk.sealed = true;
        chunk.unknown = chunk_state == 1;
        chunk.granted[0] = granted;
        chunk.taken[0] = taken;
        chunk.left[0] = left.as_ref().map(|(epoch, nonces, done)| model::Left {
            epoch: *epoch,
            nonces: nonces.clone(),
            done: *done,
        });
        chunk
    });
    let p_chunk = (chunk_state > 0).then(|| prod::ChunkCell {
        unknown: chunk_state == 1,
        granted,
        taken: taken.map(name),
        left: left.as_ref().map(|(epoch, nonces, done)| LeftEntry {
            epoch: u64::from(*epoch),
            nonces: nonces.iter().copied().map(name).collect(),
            done: done.map(name),
        }),
    });
    let (m_rec, p_rec) = match rec_state {
        0 => (None, None),
        state => {
            let form = match state {
                3 => model::Form::First,
                4 => model::Form::Leave,
                _ => model::Form::Plain,
            };
            let by = if state == 2 { OTHER } else { MINTER };
            let epoch = if state == 3 || state == 4 { 3 } else { 1 };
            (
                Some(model::Rec {
                    by,
                    epoch,
                    form,
                    nonce: 1,
                    over: over.clone(),
                    leave: 1,
                    released: state == 5,
                    token: 0,
                }),
                Some(prod::Rec {
                    by_minter: by == MINTER,
                    epoch: u64::from(epoch),
                    form: match form {
                        model::Form::Plain => prod::Form::Plain,
                        model::Form::First => prod::Form::First,
                        model::Form::Leave => prod::Form::Leave,
                    },
                    nonce: name(1),
                    over: over.iter().copied().map(name).collect(),
                    leave: name(1),
                    released: state == 5,
                }),
            )
        }
    };
    let expected = model::decide(
        design,
        &model::View {
            cell: 0,
            rec: m_rec.as_ref(),
            hdr: m_hdr.as_ref(),
            chunk: m_chunk.as_ref(),
            flag: model::Mode::On,
            author_live,
            minter: MINTER,
            placed: false,
        },
    );
    let got = prod::decide(&prod::View {
        rec: p_rec.as_ref(),
        hdr: p_hdr.as_ref(),
        chunk: p_chunk.as_ref(),
        author_live,
        placed: false,
    });
    assert!(
        same(expected, got.clone()),
        "model {expected:?}, production {got:?}: mode {mode} hdr {hdr_state} chunk {chunk_state} granted {granted} taken {taken:?} left {left:?} rec {rec_state} live {author_live} over {over:?}"
    );
    1
}

/// P3: a birth-form record that no grant covers is replaced only over a cell
/// without LTX; with LTX the record was created over a lost one and is refused.
/// A birth-form record that a grant covers, or any other take, is not touched.
#[test]
fn a_birth_form_record_without_a_grant_is_refused_over_a_cell_with_ltx() {
    let replace = prod::Take::FirstOverRecord { epoch: 2 };
    assert_eq!(prod::birth_guard(replace.clone(), true, false), replace);
    assert!(matches!(
        prod::birth_guard(replace.clone(), true, true),
        prod::Take::Refuse(_)
    ));
    assert_eq!(
        prod::birth_guard(replace.clone(), false, true),
        replace,
        "not a birth record"
    );
    let plain = prod::Take::Plain { epoch: 2 };
    assert_eq!(
        prod::birth_guard(plain.clone(), true, true),
        plain,
        "granted: a plain takeover"
    );
}

mod compare_with_on_tests {
    use crate::session::LeftEntry;
    use crate::session_take::*;

    fn rec(form: Form) -> Rec {
        Rec {
            by_minter: true,
            epoch: 1,
            form,
            nonce: "n".into(),
            over: Vec::new(),
            leave: "l".into(),
            released: false,
        }
    }

    fn chunk(granted: bool, left: Option<LeftEntry>) -> ChunkCell {
        ChunkCell {
            unknown: false,
            granted,
            taken: None,
            left,
        }
    }

    fn agreement(mode: Mode, rec: Option<&Rec>, chunk: &ChunkCell) -> Agreement {
        let hdr = Hdr { mode, lost: false };
        compare_with_on(&View {
            rec,
            hdr: Some(&hdr),
            chunk: Some(chunk),
            author_live: false,
            placed: false,
        })
    }

    #[test]
    fn a_plain_record_or_a_new_cell_reads_the_same_in_every_mode() {
        for mode in [Mode::Shadow, Mode::Read, Mode::On] {
            assert_eq!(
                agreement(mode, Some(&rec(Form::Plain)), &chunk(true, None)),
                Agreement::Same,
                "{mode:?}"
            );
            assert_eq!(
                agreement(mode, None, &chunk(false, None)),
                Agreement::Same,
                "{mode:?}"
            );
        }
    }

    #[test]
    fn a_granted_id_without_a_record_differs_by_design_below_on() {
        for mode in [Mode::Shadow, Mode::Read] {
            assert_eq!(
                agreement(mode, None, &chunk(true, None)),
                Agreement::ByDesign,
                "{mode:?}"
            );
        }
        assert_eq!(
            agreement(Mode::On, None, &chunk(true, None)),
            Agreement::Same
        );
    }

    #[test]
    fn a_leave_form_record_under_a_header_that_writes_none_is_a_mismatch() {
        let left = LeftEntry {
            epoch: 1,
            nonces: vec!["l".into()],
            done: None,
        };
        for mode in [Mode::Shadow, Mode::Read] {
            assert_eq!(
                agreement(
                    mode,
                    Some(&rec(Form::Leave)),
                    &chunk(true, Some(left.clone()))
                ),
                Agreement::Mismatch,
                "{mode:?}"
            );
        }
    }
}
