open OUnit2
open Core

(* General Test Runner *)

let run_test test_name =
  let test_domains = [| 1; 2; 4 |] in
  let test_input_filename = Printf.sprintf "../../../test/data/%s.txt" test_name in
  let test_output_filename = Printf.sprintf "../../../test/data/%s.out" test_name in
  let expected_filename = Printf.sprintf "../../../test/data/%s.expected" test_name in
  Array.iter test_domains ~f:(fun num_domains ->
    Weather_stats.get_weather_stats
      ~filename:test_input_filename
      ~use_std_out:false
      ~num_domains;
    let actual = In_channel.read_lines test_output_filename in
    let expected = In_channel.read_lines expected_filename in
    assert_equal expected actual)
;;

(*
   Test suite that is taken from: https://github.com/gunnarmorling/1brc/tree/main/src/test/resources/samples
*)
let suite =
  "suite"
  >::: [ ("test1" >:: fun _ -> run_test "test_1")
       ; ("test2" >:: fun _ -> run_test "measurements-1")
       ; ("test3" >:: fun _ -> run_test "measurements-2")
       ; ("test4" >:: fun _ -> run_test "measurements-3")
       ; ("test5" >:: fun _ -> run_test "measurements-10")
       ; ("test6" >:: fun _ -> run_test "measurements-20")
       ; ("test7" >:: fun _ -> run_test "measurements-10000-unique-keys")
       ; ("test8" >:: fun _ -> run_test "measurements-boundaries")
       ; ("test9" >:: fun _ -> run_test "measurements-complex-utf8")
       ; ("test10" >:: fun _ -> run_test "measurements-dot")
       ; ("test11" >:: fun _ -> run_test "measurements-rounding")
       ; ("test12" >:: fun _ -> run_test "measurements-short")
       ; ("test13" >:: fun _ -> run_test "measurements-shortest")
       ]
;;

let () = run_test_tt_main suite
