let main =
  let input_file = Sys.argv.(1) in
  let num_domains = Sys.argv.(2) |> int_of_string in
  Weather_stats.get_weather_stats ~filename:input_file ~use_std_out:true ~num_domains
;;

let _ = main
