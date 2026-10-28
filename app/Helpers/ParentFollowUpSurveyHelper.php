<?php

namespace App\Helpers;

class ParentFollowUpSurveyHelper
{

        /**
     * Balance course survey value → label mappings.
     * Includes question and sub-question labels.
     */
    const PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS = [
        // balance_q1 – frequency of cycle use
        'balance_q1'     => 'Q.1 How frequently does your child use a cycle for each of the following reasons',
        'balance_q1a'    => 'Q.1a To go to/from school',
        'balance_q1a_o1' => '4 or more days a week',
        'balance_q1a_o2' => 'Between 1 to 3 days a week',
        'balance_q1a_o3' => 'Once or twice a month',
        'balance_q1a_o4' => 'Less than once a month',
        'balance_q1a_o5' => 'Never',
        'balance_q1a_o6' => "Not applicable (my child doesn't know how to use a cycle yet)",

        'balance_q1b'    => 'Q.1b Leisure/social reasons',
        'balance_q1b_o1' => '4 or more days a week',
        'balance_q1b_o2' => 'Between 1 to 3 days a week',
        'balance_q1b_o3' => 'Once or twice a month',
        'balance_q1b_o4' => 'Less than once a month',
        'balance_q1b_o5' => 'Never',
        'balance_q1b_o6' => "Not applicable (my child doesn't know how to use a cycle yet)",

        'balance_q1c'    => 'Q.1c Exercise',
        'balance_q1c_o1' => '4 or more days a week',
        'balance_q1c_o2' => 'Between 1 to 3 days a week',
        'balance_q1c_o3' => 'Once or twice a month',
        'balance_q1c_o4' => 'Less than once a month',
        'balance_q1c_o5' => 'Never',
        'balance_q1c_o6' => "Not applicable (my child doesn't know how to use a cycle yet)",

        // balance_q2 – confidence in child’s ability to use a cycle
        'balance_q2'     => "Q.2 How confident do you feel in your child's ability to:",
        'balance_q2a'    => 'Q.2a Use a cycle',
        'balance_q2a_o1' => 'Very confident',
        'balance_q2a_o2' => 'Quite confident',
        'balance_q2a_o3' => 'Neither confident nor unconfident',
        'balance_q2a_o4' => 'Not very confident',
        'balance_q2a_o5' => 'Very unconfident',
        'balance_q2a_o6' => 'Unsure',

        // balance_q3 – likelihood to encourage child to use a cycle
        'balance_q3'     => 'Q.3 How likely are you to encourage your child to do the following?',
        'balance_q3a'    => 'Q.3a Use a cycle',
        'balance_q3a_o1' => 'Very likely',
        'balance_q3a_o2' => 'Likely',
        'balance_q3a_o3' => 'Unlikely',
        'balance_q3a_o4' => 'Very unlikely',
        'balance_q3a_o5' => 'Unsure',

        // balance_q4 – how much child would like to use cycle
        'balance_q4'     => 'Q.4 Please ask your child the following question: how much would you like to use your cycle?',
        'balance_q4a'    => 'Q.4a How much would your child like to use their cycle?',
        'balance_q4a_o1' => 'A lot more than I do now',
        'balance_q4a_o2' => 'More than I do now',
        'balance_q4a_o3' => 'About the same amount as I do now',
        'balance_q4a_o4' => 'Less than I do now',
        'balance_q4a_o5' => 'A lot less than I do now',

        // balance_q5 – what would encourage child to use a cycle more
        'balance_q5'     => 'Q.5 Which, if any, of the following would encourage your child to use a cycle more often than they currently do? Please select all that apply.',
        'balance_q5a_o1' => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
        'balance_q5a_o2' => 'Presence and awareness of local cycle routes',
        'balance_q5a_o3' => 'Secure storage at school or home',
        'balance_q5a_o4' => 'Smoother road surfaces for cycling',
        'balance_q5a_o5' => 'Me, the parent/carer, having greater confidence using a cycle',
        'balance_q5a_o6' => 'Cycle maintenance or repair courses',
        'balance_q5a_o7' => 'Local initiatives that facilitate cycling (for example, group rides)',
        'balance_q5a_o8' => 'Ability to purchase a cycle',
        'balance_q5a_o9' => 'My child does not want to cycle more',
        'balance_q5a_o10' => 'None of the above',

        // balance_q6 – physical activity levels
        'balance_q6'     => "Q.6 How would you describe your child's physical activity levels compared to 6 months ago? Are they...",
        'balance_q6a_o1' => 'A lot more physically active?',
        'balance_q6a_o2' => 'A little more physically active?',
        'balance_q6a_o3' => 'About as active as they were 6 months ago?',
        'balance_q6a_o4' => 'A little less physically active?',
        'balance_q6a_o5' => 'A lot less physically active?',
    // ];

    /**
     * Learn to Ride course survey value → label mappings.
     * Includes question and sub-question labels.
     */
    // const learn_to_ride = [
        // learn_to_ride_q1 – frequency of cycle use
        'learn_to_ride_q1'     => 'Q.1 How frequently does your child use a cycle for each of the following reasons',
        'learn_to_ride_q1a'    => 'Q.1a To go to/from school',
        'learn_to_ride_q1a_o1' => '4 or more days a week',
        'learn_to_ride_q1a_o2' => 'Between 1 to 3 days a week',
        'learn_to_ride_q1a_o3' => 'Once or twice a month',
        'learn_to_ride_q1a_o4' => 'Less than once a month',
        'learn_to_ride_q1a_o5' => 'Never',
        'learn_to_ride_q1a_o6' => "Not applicable (my child doesn't know how to use a cycle yet)",

        'learn_to_ride_q1b'    => 'Q.1b Leisure/social reasons',
        'learn_to_ride_q1b_o1' => '4 or more days a week',
        'learn_to_ride_q1b_o2' => 'Between 1 to 3 days a week',
        'learn_to_ride_q1b_o3' => 'Once or twice a month',
        'learn_to_ride_q1b_o4' => 'Less than once a month',
        'learn_to_ride_q1b_o5' => 'Never',
        'learn_to_ride_q1b_o6' => "Not applicable (my child doesn't know how to use a cycle yet)",

        'learn_to_ride_q1c'    => 'Q.1c Exercise',
        'learn_to_ride_q1c_o1' => '4 or more days a week',
        'learn_to_ride_q1c_o2' => 'Between 1 to 3 days a week',
        'learn_to_ride_q1c_o3' => 'Once or twice a month',
        'learn_to_ride_q1c_o4' => 'Less than once a month',
        'learn_to_ride_q1c_o5' => 'Never',
        'learn_to_ride_q1c_o6' => "Not applicable (my child doesn't know how to use a cycle yet)",

        // learn_to_ride_q2 – confidence in child’s ability to use a cycle
        'learn_to_ride_q2'     => "Q.2 How confident do you feel in your child's ability to:",
        'learn_to_ride_q2a'    => 'Q.2a Use a cycle',
        'learn_to_ride_q2a_o1' => 'Very confident',
        'learn_to_ride_q2a_o2' => 'Quite confident',
        'learn_to_ride_q2a_o3' => 'Neither confident nor unconfident',
        'learn_to_ride_q2a_o4' => 'Not very confident',
        'learn_to_ride_q2a_o5' => 'Very unconfident',
        'learn_to_ride_q2a_o6' => 'Unsure',

        // learn_to_ride_q3 – likelihood to encourage child to use a cycle
        'learn_to_ride_q3'     => 'Q.3 How likely are you to encourage your child to do the following?',
        'learn_to_ride_q3a'    => 'Q.3a Use a cycle',
        'learn_to_ride_q3a_o1' => 'Very likely',
        'learn_to_ride_q3a_o2' => 'Likely',
        'learn_to_ride_q3a_o3' => 'Unlikely',
        'learn_to_ride_q3a_o4' => 'Very unlikely',
        'learn_to_ride_q3a_o5' => 'Unsure',

        // learn_to_ride_q4 – how much child would like to use cycle
        'learn_to_ride_q4'     => 'Q.4 Please ask your child the following question: how much would you like to use your cycle?',
        'learn_to_ride_q4a'    => 'Q.4a How much would you like to use your cycle?',
        'learn_to_ride_q4a_o1' => 'A lot more than I do now',
        'learn_to_ride_q4a_o2' => 'More than I do now',
        'learn_to_ride_q4a_o3' => 'About the same amount as I do now',
        'learn_to_ride_q4a_o4' => 'Less than I do now',
        'learn_to_ride_q4a_o5' => 'A lot less than I do now',

        // learn_to_ride_q5 – what would encourage child to use a cycle more
        'learn_to_ride_q5'     => 'Q.5 Which, if any, of the following would encourage your child to use a cycle more often than they currently do? Please select all that apply.',
        'learn_to_ride_q5a_o1' => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
        'learn_to_ride_q5a_o2' => 'Presence and awareness of local cycle routes',
        'learn_to_ride_q5a_o3' => 'Secure storage at school or home',
        'learn_to_ride_q5a_o4' => 'Smoother road surfaces for cycling',
        'learn_to_ride_q5a_o5' => 'Me, the parent/carer, having greater confidence using a cycle',
        'learn_to_ride_q5a_o6' => 'Cycle maintenance or repair courses',
        'learn_to_ride_q5a_o7' => 'Local initiatives that facilitate cycling (for example, group rides)',
        'learn_to_ride_q5a_o8' => 'Ability to purchase a cycle',
        'learn_to_ride_q5a_o9' => 'My child does not want to cycle more',
        'learn_to_ride_q5a_o10' => 'None of the above',

        // learn_to_ride_q6 – physical activity levels
        'learn_to_ride_q6'     => "Q.6 How would you describe your child's physical activity levels compared to 6 months ago? Are they...",
        'learn_to_ride_q6a_o1' => 'A lot more physically active?',
        'learn_to_ride_q6a_o2' => 'A little more physically active?',
        'learn_to_ride_q6a_o3' => 'About as active as they were 6 months ago?',
        'learn_to_ride_q6a_o4' => 'A little less physically active?',
        'learn_to_ride_q6a_o5' => 'A lot less physically active?',
    // ];

    /**
     * Level 1 course survey value → label mappings.
     * Includes question and sub-question labels.
     */
    // const level_1 = [
        // level_1_q1 – frequency of cycle use
        'level_1_q1'     => 'Q.1 How frequently does your child use a cycle for each of the following reasons',
        'level_1_q1a'    => 'Q.1a To go to/from school',
        'level_1_q1a_o1' => '4 or more days a week',
        'level_1_q1a_o2' => 'Between 1 to 3 days a week',
        'level_1_q1a_o3' => 'Once or twice a month',
        'level_1_q1a_o4' => 'Less than once a month',
        'level_1_q1a_o5' => 'Never',

        'level_1_q1b'    => 'Q.1b Leisure/social reasons',
        'level_1_q1b_o1' => '4 or more days a week',
        'level_1_q1b_o2' => 'Between 1 to 3 days a week',
        'level_1_q1b_o3' => 'Once or twice a month',
        'level_1_q1b_o4' => 'Less than once a month',
        'level_1_q1b_o5' => 'Never',

        'level_1_q1c'    => 'Q.1c Exercise',
        'level_1_q1c_o1' => '4 or more days a week',
        'level_1_q1c_o2' => 'Between 1 to 3 days a week',
        'level_1_q1c_o3' => 'Once or twice a month',
        'level_1_q1c_o4' => 'Less than once a month',
        'level_1_q1c_o5' => 'Never',

        // level_1_q2 – confidence
        'level_1_q2'     => "Q.2 How confident do you feel in your child's ability to:",
        'level_1_q2a'    => 'Q.2a Use a cycle',
        'level_1_q2a_o1' => 'Very confident',
        'level_1_q2a_o2' => 'Quite confident',
        'level_1_q2a_o3' => 'Neither confident nor unconfident',
        'level_1_q2a_o4' => 'Not very confident',
        'level_1_q2a_o5' => 'Very unconfident',
        'level_1_q2a_o6' => 'Unsure',

        'level_1_q2b'    => 'Q.2b Cycle independently',
        'level_1_q2b_o1' => 'Very confident',
        'level_1_q2b_o2' => 'Quite confident',
        'level_1_q2b_o3' => 'Neither confident nor unconfident',
        'level_1_q2b_o4' => 'Not very confident',
        'level_1_q2b_o5' => 'Very unconfident',
        'level_1_q2b_o6' => 'Unsure',

        // level_1_q3 – likelihood to encourage child to use a cycle
        'level_1_q3'     => 'Q.3 How likely are you to encourage your child to do the following?',
        'level_1_q3a'    => 'Q.3a Use a cycle',
        'level_1_q3a_o1' => 'Very likely',
        'level_1_q3a_o2' => 'Likely',
        'level_1_q3a_o3' => 'Unlikely',
        'level_1_q3a_o4' => 'Very unlikely',
        'level_1_q3a_o5' => 'Unsure',

        // level_1_q4 – how much child would like to use cycle (shares option codes with q5)
        // 'level_1_q4'  => 'Q.4 Please ask your child the following question: how much would you like to use your cycle?',
        'level_1_q4'    => 'Q.4 How much would your child like to use their cycle?',
        'level_1_q4_o1' => 'A lot more than I do now',
        'level_1_q4_o2' => 'More than I do now',
        'level_1_q4_o3' => 'About the same amount as I do now',
        'level_1_q4_o4' => 'Less than I do now',
        'level_1_q4_o5' => 'A lot less than I do now',

        // level_1_q5 – what would encourage child to use a cycle more
        // note: q4 radios reuse the same value codes level_1_q5_o1–level_1_q5_o5;
        // this array maps them according to the checkbox labels from q5.
        'level_1_q5'    => 'Q.5 Which, if any, of the following would encourage your child to use a cycle more often than they currently do? Please select all that apply.',
        'level_1_q5_o1' => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
        'level_1_q5_o2' => 'Presence and awareness of local cycle routes',
        'level_1_q5_o3' => 'Secure storage at school or home',
        'level_1_q5_o4' => 'Smoother road surfaces for cycling',
        'level_1_q5_o5' => 'Me, the parent/carer, having greater confidence using a cycle',
        'level_1_q5_o6' => 'Cycle maintenance or repair courses',
        'level_1_q5_o7' => 'Local initiatives that facilitate cycling (for example, group rides)',
        'level_1_q5_o8' => 'Ability to purchase a cycle',
        'level_1_q5_o9' => 'My child does not want to cycle more',
        'level_1_q5_o10' => 'None of the above',

        // level_1_q6 – physical activity levels
        'level_1_q6'    => "Q.6 How would you describe your child's physical activity levels compared to 6 months ago? Are they...",
        'level_1_q6_o1' => 'A lot more physically active?',
        'level_1_q6_o2' => 'A little more physically active?',
        'level_1_q6_o3' => 'About as active as they were 6 months ago?',
        'level_1_q6_o4' => 'A little less physically active?',
        'level_1_q6_o5' => 'A lot less physically active?',
    // ];

    /**
     * Level 2 course survey value → label mappings.
     * Includes question and sub-question labels.
     */
    // const level_2 = [
        // level_2_q1 – frequency of cycle use
        'level_2_q1'     => 'Q.1 How frequently does your child use a cycle for each of the following reasons',
        'level_2_q1a'    => 'Q.1a To go to/from school',
        'level_2_q1a_o1' => '4 or more days a week',
        'level_2_q1a_o2' => 'Between 1 to 3 days a week',
        'level_2_q1a_o3' => 'Once or twice a month',
        'level_2_q1a_o4' => 'Less than once a month',
        'level_2_q1a_o5' => 'Never',

        'level_2_q1b'    => 'Q.1b Leisure/social reasons',
        'level_2_q1b_o1' => '4 or more days a week',
        'level_2_q1b_o2' => 'Between 1 to 3 days a week',
        'level_2_q1b_o3' => 'Once or twice a month',
        'level_2_q1b_o4' => 'Less than once a month',
        'level_2_q1b_o5' => 'Never',

        'level_2_q1c'    => 'Q.1c Exercise',
        'level_2_q1c_o1' => '4 or more days a week',
        'level_2_q1c_o2' => 'Between 1 to 3 days a week',
        'level_2_q1c_o3' => 'Once or twice a month',
        'level_2_q1c_o4' => 'Less than once a month',
        'level_2_q1c_o5' => 'Never',

        // level_2_q2 – confidence
        'level_2_q2'     => "Q.2 How confident do you feel in your child's ability to:",
        'level_2_q2a'    => 'Q.2a Cycle on roads',
        'level_2_q2a_o1' => 'Very confident',
        'level_2_q2a_o2' => 'Quite confident',
        'level_2_q2a_o3' => 'Neither confident nor unconfident',
        'level_2_q2a_o4' => 'Not very confident',
        'level_2_q2a_o5' => 'Very unconfident',
        'level_2_q2a_o6' => 'Unsure',

        'level_2_q2b'    => 'Q.2b Cycle independently',
        'level_2_q2b_o1' => 'Very confident',
        'level_2_q2b_o2' => 'Quite confident',
        'level_2_q2b_o3' => 'Neither confident nor unconfident',
        'level_2_q2b_o4' => 'Not very confident',
        'level_2_q2b_o5' => 'Very unconfident',
        'level_2_q2b_o6' => 'Unsure',

        // level_2_q3 – likelihood to encourage
        'level_2_q3'     => 'Q.3 How likely are you to encourage your child to do the following?',
        'level_2_q3a'    => 'Q.3a Use a cycle',
        'level_2_q3a_o1' => 'Very likely',
        'level_2_q3a_o2' => 'Likely',
        'level_2_q3a_o3' => 'Unlikely',
        'level_2_q3a_o4' => 'Very unlikely',
        'level_2_q3a_o5' => 'Unsure',

        'level_2_q3b'    => 'Q.3b Cycle on roads',
        'level_2_q3b_o1' => 'Very likely',
        'level_2_q3b_o2' => 'Likely',
        'level_2_q3b_o3' => 'Unlikely',
        'level_2_q3b_o4' => 'Very unlikely',
        'level_2_q3b_o5' => 'Unsure',

        // level_2_q4 – perceived safety cycling on roads
        'level_2_q4'    => 'Q.4 How safe do you feel your child is cycling on roads by themself?',
        'level_2_q4_o1' => 'Very safe',
        'level_2_q4_o2' => 'Safe',
        'level_2_q4_o3' => 'Unsafe',
        'level_2_q4_o4' => 'Very unsafe',
        'level_2_q4_o5' => 'Unsure',

        // level_2_q5 – how much child would like to use cycle
        'level_2_q5'    => 'Q.5 Please ask your child the following question: how much would you like to use your cycle?',
        'level_2_q5_o1' => 'A lot more than I do now',
        'level_2_q5_o2' => 'More than I do now',
        'level_2_q5_o3' => 'About the same amount as I do now',
        'level_2_q5_o4' => 'Less than I do now',
        'level_2_q5_o5' => 'A lot less than I do now',

        // level_2_q6 – what would encourage child to use a cycle more
        'level_2_q6'    => 'Q.6 Which, if any, of the following would encourage your child to use a cycle more often than they currently do? Please select all that apply.',
        'level_2_q6_o1' => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
        'level_2_q6_o2' => 'Presence and awareness of local cycle routes',
        'level_2_q6_o3' => 'Secure storage at school or home',
        'level_2_q6_o4' => 'Smoother road surfaces for cycling',
        'level_2_q6_o5' => 'Me, the parent/carer, having greater confidence using a cycle',
        'level_2_q6_o6' => 'Cycle maintenance or repair courses',
        'level_2_q6_o7' => 'Local initiatives that facilitate cycling (for example, group rides)',
        'level_2_q6_o8' => 'Ability to purchase a cycle',
        'level_2_q6_o9' => 'My child does not want to cycle more',
        'level_2_q6_o10' => 'None of the above',

        // level_2_q7 – change in confidence since training
        'level_2_q7'    => "Q.7 Since taking part in cycle training, has your child's confidence in their cycling ability…",
        'level_2_q7_o1' => 'Increased a lot',
        'level_2_q7_o2' => 'Increased a little',
        'level_2_q7_o3' => 'Stayed about the same',
        'level_2_q7_o4' => 'Decreased a little',
        'level_2_q7_o5' => 'Decreased a lot',

        // level_2_q8 – physical activity levels
        'level_2_q8'    => "Q.8 How would you describe your child's physical activity levels compared to 6 months ago? Are they...",
        'level_2_q8_o1' => 'A lot more physically active?',
        'level_2_q8_o2' => 'A little more physically active?',
        'level_2_q8_o3' => 'About as active as they were 6 months ago?',
        'level_2_q8_o4' => 'A little less physically active?',
        'level_2_q8_o5' => 'A lot less physically active?',

        // level_2_q9 – virtual rider training
        'level_2_q9'    => 'Q.9 Has your child had a go at the Virtual Rider Training tool?',
        'level_2_q9_o1' => 'Yes',
        'level_2_q9_o2' => 'No',

        // level_1_2_q10 – how do you feel about cycling on the road
        'level_2_q10'    => 'Now that you’ve had a go at the challenges, how do you feel about cycling on the road?',
        'level_2_q10_o1' => 'A lot more confident',
        'level_2_q10_o2' => 'More confident',
        'level_2_q10_o3' => 'Less confident',
        'level_2_q10_o4' => 'A lot less confident',
        'level_2_q10_o5' => 'The challenges didn\'t change my level of confidence',

        // level_2_q11 – new cycling skills
        'level_2_q11'    => '11. Did you learn any new cycling skills that you did not already know or had forgotten? Select all that apply:',
        'level_2_q11_o1' => 'Where to position myself on the road',
        'level_2_q11_o2' => 'How to communicate with other road users',
        'level_2_q11_o3' => 'Where I should be looking',
        'level_2_q11_o4' => 'Understanding who has priority in different situations',
        'level_2_q11_o5' => 'Other',
        'level_2_q11_o6' => 'None of the above',

        // level_2_q12 – new cycling skills
        'level_2_q12'    => '12. How likely are you to cycle on roads now that you’ve had a go at the challenges?',
        'level_2_q12_o1' => 'Much more likely',
        'level_2_q12_o2' => 'More likely',
        'level_2_q12_o3' => 'About the same',
        'level_2_q12_o4' => 'Less likely',
        'level_2_q12_o5' => 'Much less likely',

        // level_2_q13 – new cycling skills
        'level_2_q13'    => '13. How much more likely are you, the parent/carer, to let your child cycle on roads now?',
        'level_2_q13_o1' => 'Much more likely',
        'level_2_q13_o2' => 'More likely',
        'level_2_q13_o3' => 'About the same',
        'level_2_q13_o4' => 'Less likely',
        'level_2_q13_o5' => 'Much less likely',
    // ];

    /**
     * Level 1 & 2 combined course survey value → label mappings.
     * Includes question and sub-question labels.
     */
    // const level_1_and_2 = [
        // level_1_and_2_q1 – frequency of cycle use
        'level_1_2_q1'     => 'Q.1 How frequently does your child use a cycle for each of the following reasons',
        'level_1_2_q1a'    => 'Q.1a To go to/from school',
        'level_1_2_q1a_o1' => '4 or more days a week',
        'level_1_2_q1a_o2' => 'Between 1 to 3 days a week',
        'level_1_2_q1a_o3' => 'Once or twice a month',
        'level_1_2_q1a_o4' => 'Less than once a month',
        'level_1_2_q1a_o5' => 'Never',

        'level_1_2_q1b'    => 'Q.1b Leisure/social reasons',
        'level_1_2_q1b_o1' => '4 or more days a week',
        'level_1_2_q1b_o2' => 'Between 1 to 3 days a week',
        'level_1_2_q1b_o3' => 'Once or twice a month',
        'level_1_2_q1b_o4' => 'Less than once a month',
        'level_1_2_q1b_o5' => 'Never',

        'level_1_2_q1c'    => 'Q.1c Exercise',
        'level_1_2_q1c_o1' => '4 or more days a week',
        'level_1_2_q1c_o2' => 'Between 1 to 3 days a week',
        'level_1_2_q1c_o3' => 'Once or twice a month',
        'level_1_2_q1c_o4' => 'Less than once a month',
        'level_1_2_q1c_o5' => 'Never',

        // level_1_2_q2 – confidence
        'level_1_2_q2'     => "Q.2 How confident do you feel in your child's ability to:",
        'level_1_2_q2a'    => 'Q.2a Cycle on roads',
        'level_1_2_q2a_o1' => 'Very confident',
        'level_1_2_q2a_o2' => 'Quite confident',
        'level_1_2_q2a_o3' => 'Neither confident nor unconfident',
        'level_1_2_q2a_o4' => 'Not very confident',
        'level_1_2_q2a_o5' => 'Very unconfident',
        'level_1_2_q2a_o6' => 'Unsure',

        'level_1_2_q2b'    => 'Q.2b Cycle independently',
        'level_1_2_q2b_o1' => 'Very confident',
        'level_1_2_q2b_o2' => 'Quite confident',
        'level_1_2_q2b_o3' => 'Neither confident nor unconfident',
        'level_1_2_q2b_o4' => 'Not very confident',
        'level_1_2_q2b_o5' => 'Very unconfident',
        'level_1_2_q2b_o6' => 'Unsure',

        // level_1_2_q3 – likelihood to encourage
        'level_1_2_q3'     => 'Q.3 How likely are you to encourage your child to do the following?',
        'level_1_2_q3a'    => 'Q.3a Use a cycle',
        'level_1_2_q3a_o1' => 'Very likely',
        'level_1_2_q3a_o2' => 'Likely',
        'level_1_2_q3a_o3' => 'Unlikely',
        'level_1_2_q3a_o4' => 'Very unlikely',
        'level_1_2_q3a_o5' => 'Unsure',

        'level_1_2_q3b'    => 'Q.3b Cycle on roads',
        'level_1_2_q3b_o1' => 'Very likely',
        'level_1_2_q3b_o2' => 'Likely',
        'level_1_2_q3b_o3' => 'Unlikely',
        'level_1_2_q3b_o4' => 'Very unlikely',
        'level_1_2_q3b_o5' => 'Unsure',

        // level_1_2_q4 – perceived safety cycling on roads
        'level_1_2_q4'    => 'Q.4 How safe do you feel your child is cycling on roads by themself?',
        'level_1_2_q4_o1' => 'Very safe',
        'level_1_2_q4_o2' => 'Safe',
        'level_1_2_q4_o3' => 'Unsafe',
        'level_1_2_q4_o4' => 'Very unsafe',
        'level_1_2_q4_o5' => 'Unsure',

        // level_2_q5 – how much child would like to use cycle
        'level_1_2_q5'    => 'Q.5 Please ask your child the following question: how much would you like to use your cycle?',
        'level_1_2_q5_o1' => 'A lot more than I do now',
        'level_1_2_q5_o2' => 'More than I do now',
        'level_1_2_q5_o3' => 'About the same amount as I do now',
        'level_1_2_q5_o4' => 'Less than I do now',
        'level_1_2_q5_o5' => 'A lot less than I do now',

        // level_1_2_q6 – what would encourage child to use a cycle more
        'level_1_2_q6'    => 'Q.6 Which, if any, of the following would encourage your child to use a cycle more often than they currently do? Please select all that apply.',
        'level_1_2_q6_o1' => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
        'level_1_2_q6_o2' => 'Presence and awareness of local cycle routes',
        'level_1_2_q6_o3' => 'Secure storage at school or home',
        'level_1_2_q6_o4' => 'Smoother road surfaces for cycling',
        'level_1_2_q6_o5' => 'Me, the parent/carer, having greater confidence using a cycle',
        'level_1_2_q6_o6' => 'Cycle maintenance or repair courses',
        'level_1_2_q6_o7' => 'Local initiatives that facilitate cycling (for example, group rides)',
        'level_1_2_q6_o8' => 'Ability to purchase a cycle',
        'level_1_2_q6_o9' => 'My child does not want to cycle more',
        'level_1_2_q6_o10' => 'None of the above',

        // level_1_2_q7 – change in confidence since training
        'level_1_2_q7'    => "Q.7 Since taking part in cycle training, has your child's confidence in their cycling ability…",
        'level_1_2_q7_o1' => 'Increased a lot',
        'level_1_2_q7_o2' => 'Increased a little',
        'level_1_2_q7_o3' => 'Stayed about the same',
        'level_1_2_q7_o4' => 'Decreased a little',
        'level_1_2_q7_o5' => 'Decreased a lot',

        // level_1_2_q8 – physical activity levels
        'level_1_2_q8'    => "Q.8 How would you describe your child's physical activity levels compared to 6 months ago? Are they...",
        'level_1_2_q8_o1' => 'A lot more physically active?',
        'level_1_2_q8_o2' => 'A little more physically active?',
        'level_1_2_q8_o3' => 'About as active as they were 6 months ago?',
        'level_1_2_q8_o4' => 'A little less physically active?',
        'level_1_2_q8_o5' => 'A lot less physically active?',
    // ];

    /**
     * Level 3 course survey value → label mappings.
     * Includes question and sub-question labels.
     */
    // const level_3 = [
        // level_3_q1 – frequency of cycle use
        'level_3_q1'     => 'Q.1 How frequently does your child use a cycle for each of the following reasons',
        'level_3_q1a'    => 'Q.1a To go to/from school',
        'level_3_q1a_o1' => '4 or more days a week',
        'level_3_q1a_o2' => 'Between 1 to 3 days a week',
        'level_3_q1a_o3' => 'Once or twice a month',
        'level_3_q1a_o4' => 'Less than once a month',
        'level_3_q1a_o5' => 'Never',

        'level_3_q1b'    => 'Q.1b Leisure/social reasons',
        'level_3_q1b_o1' => '4 or more days a week',
        'level_3_q1b_o2' => 'Between 1 to 3 days a week',
        'level_3_q1b_o3' => 'Once or twice a month',
        'level_3_q1b_o4' => 'Less than once a month',
        'level_3_q1b_o5' => 'Never',

        'level_3_q1c'    => 'Q.1c Exercise',
        'level_3_q1c_o1' => '4 or more days a week',
        'level_3_q1c_o2' => 'Between 1 to 3 days a week',
        'level_3_q1c_o3' => 'Once or twice a month',
        'level_3_q1c_o4' => 'Less than once a month',
        'level_3_q1c_o5' => 'Never',

        // level_3_q2 – confidence
        'level_3_q2'     => "Q.2 How confident do you feel in your child's ability to:",
        'level_3_q2a'    => 'Q.2a Cycle on roads',
        'level_3_q2a_o1' => 'Very confident',
        'level_3_q2a_o2' => 'Quite confident',
        'level_3_q2a_o3' => 'Neither confident nor unconfident',
        'level_3_q2a_o4' => 'Not very confident',
        'level_3_q2a_o5' => 'Very unconfident',
        'level_3_q2a_o6' => 'Unsure',

        'level_3_q2b'    => 'Q.2b Cycle independently',
        'level_3_q2b_o1' => 'Very confident',
        'level_3_q2b_o2' => 'Quite confident',
        'level_3_q2b_o3' => 'Neither confident nor unconfident',
        'level_3_q2b_o4' => 'Not very confident',
        'level_3_q2b_o5' => 'Very unconfident',
        'level_3_q2b_o6' => 'Unsure',

        // level_3_q3 – likelihood to encourage
        'level_3_q3'     => 'Q.3 How likely are you to encourage your child to do the following?',
        'level_3_q3a'    => 'Q.3a Use a cycle',
        'level_3_q3a_o1' => 'Very likely',
        'level_3_q3a_o2' => 'Likely',
        'level_3_q3a_o3' => 'Unlikely',
        'level_3_q3a_o4' => 'Very unlikely',
        'level_3_q3a_o5' => 'Unsure',

        'level_3_q3b'    => 'Q.3b Cycle on roads',
        'level_3_q3b_o1' => 'Very likely',
        'level_3_q3b_o2' => 'Likely',
        'level_3_q3b_o3' => 'Unlikely',
        'level_3_q3b_o4' => 'Very unlikely',
        'level_3_q3b_o5' => 'Unsure',

        // level_3_q4 – perceived safety cycling on roads
        'level_3_q4'    => 'Q.4 How safe do you feel your child is cycling on roads by themself?',
        'level_3_q4_o1' => 'Very safe',
        'level_3_q4_o2' => 'Safe',
        'level_3_q4_o3' => 'Unsafe',
        'level_3_q4_o4' => 'Very unsafe',
        'level_3_q4_o5' => 'Unsure',

        // level_3_q5 – how much child would like to use cycle
        'level_3_q5'    => 'Q.5 Please ask your child the following question: how much would you like to use your cycle?',
        'level_3_q5_o1' => 'A lot more than I do now',
        'level_3_q5_o2' => 'More than I do now',
        'level_3_q5_o3' => 'About the same amount as I do now',
        'level_3_q5_o4' => 'Less than I do now',
        'level_3_q5_o5' => 'A lot less than I do now',

        // level_3_q6 – what would encourage child to use a cycle more
        'level_3_q6'    => 'Q.6 Which, if any, of the following would encourage your child to use a cycle more often than they currently do? Please select all that apply.',
        'level_3_q6_o1' => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
        'level_3_q6_o2' => 'Presence and awareness of local cycle routes',
        'level_3_q6_o3' => 'Secure storage at school or home',
        'level_3_q6_o4' => 'Smoother road surfaces for cycling',
        'level_3_q6_o5' => 'Me, the parent/carer, having greater confidence using a cycle',
        'level_3_q6_o6' => 'Cycle maintenance or repair courses',
        'level_3_q6_o7' => 'Local initiatives that facilitate cycling (for example, group rides)',
        'level_3_q6_o8' => 'Ability to purchase a cycle',
        'level_3_q6_o9' => 'My child does not want to cycle more',
        'level_3_q6_o10' => 'None of the above',

        // level_3_q7 – change in confidence since training
        'level_3_q7'    => "Q.7 Since taking part in cycle training, has your child's confidence in their cycling ability…",
        'level_3_q7_o1' => 'Increased a lot',
        'level_3_q7_o2' => 'Increased a little',
        'level_3_q7_o3' => 'Stayed about the same',
        'level_3_q7_o4' => 'Decreased a little',
        'level_3_q7_o5' => 'Decreased a lot',

        // level_3_q8 – physical activity levels
        'level_3_q8'    => "Q.8 How would you describe your child's physical activity levels compared to 6 months ago? Are they...",
        'level_3_q8_o1' => 'A lot more physically active?',
        'level_3_q8_o2' => 'A little more physically active?',
        'level_3_q8_o3' => 'About as active as they were 6 months ago?',
        'level_3_q8_o4' => 'A little less physically active?',
        'level_3_q8_o5' => 'A lot less physically active?',


        // level_3_q9 – virtual rider training
        'level_3_q9'    => 'Q.9 Has your child had a go at the Virtual Rider Training tool?',
        'level_3_q9_o1' => 'Yes',
        'level_3_q9_o2' => 'No',

        // level_3_q10 – how do you feel about cycling on the road
        'level_3_q10'    => 'Now that you\'ve had a go at the challenges, how do you feel about cycling on the road?',
        'level_3_q10_o1' => 'A lot more confident',
        'level_3_q10_o2' => 'More confident',
        'level_3_q10_o3' => 'Less confident',
        'level_3_q10_o4' => 'A lot less confident',
        'level_3_q10_o5' => 'The challenges didn\'t change my level of confidence',

        // level_3_q11 – new cycling skills
        'level_3_q11'    => '11. Did you learn any new cycling skills that you did not already know or had forgotten? Select all that apply:',
        'level_3_q11_o1' => 'Where to position myself on the road',
        'level_3_q11_o2' => 'How to communicate with other road users',
        'level_3_q11_o3' => 'Where I should be looking',
        'level_3_q11_o4' => 'Understanding who has priority in different situations',
        'level_3_q11_o5' => 'Other',
        'level_3_q11_o6' => 'None of the above',

        // level_3_q12 – new cycling skills
        'level_3_q12'    => '12. How likely are you to cycle on roads now that you\'ve had a go at the challenges?',
        'level_3_q12_o1' => 'Much more likely',
        'level_3_q12_o2' => 'More likely',
        'level_3_q12_o3' => 'About the same',
        'level_3_q12_o4' => 'Less likely',
        'level_3_q12_o5' => 'Much less likely',

        // level_3_q13 – new cycling skills
        'level_3_q13'    => '13. How much more likely are you, the parent/carer, to let your child cycle on roads now?',
        'level_3_q13_o1' => 'Much more likely',
        'level_3_q13_o2' => 'More likely',
        'level_3_q13_o3' => 'About the same',
        'level_3_q13_o4' => 'Less likely',
        'level_3_q13_o5' => 'Much less likely',
    ];
    public const PARENT_FOLLOW_UP_SURVEY_HEADINGS = [
        // 1) fixed metadata – same as now
        'grant_number'         => 'Grant number',
        'grant_source'         => 'Grant funding source',
        'grant_recipient'      => 'Grant recipient',
        'delivery_id'          => 'Delivery ID',
        'rider_id'             => 'Rider ID',
        'training_provider'    => 'Training provider',
        'school_organisation'  => 'School/Organisation',
        'school_urn'           => 'School URN',
        'course_name'          => 'Course name',
        'survey_received_date' => 'Received Date',
        'invitation_month'     => 'Invitation Month',

        // 2) Follow‑up course‑specific sections, grouped by course name
        'balance' => [
            'course'    => 'Balance',
            'questions' => [
                'balance_q1' => [
                    'type'          => 'frequency',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q1'],
                    'sub_questions' => [
                        'balance_q1a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q1a'],
                        'balance_q1b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q1b'],
                        'balance_q1c' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q1c'],
                    ],
                ],
                'balance_q2' => [
                    'type'          => 'confidence',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q2'],
                    'sub_questions' => [
                        'balance_q2a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q2a'],
                    ],
                ],
                'balance_q3' => [
                    'type'          => 'likelihood',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q3'],
                    'sub_questions' => [
                        'balance_q3a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q3a'],
                    ],
                ],
                'balance_q4' => [
                    'type'          => 'how_much',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q4'],
                    'sub_questions' => [
                        'balance_q4a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q4a'],
                    ],
                ],
                'balance_q5' => [
                    'type'     => 'encouragement_list',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q5'],
                ],
                'balance_q6' => [
                    'type'     => 'activity_level',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['balance_q6'],
                ],
            ],
        ],

        'learn_to_ride' => [
            'course'    => 'Learn to Ride',
            'questions' => [
                'learn_to_ride_q1' => [
                    'type'          => 'frequency',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q1'],
                    'sub_questions' => [
                        'learn_to_ride_q1a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q1a'],
                        'learn_to_ride_q1b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q1b'],
                        'learn_to_ride_q1c' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q1c'],
                    ],
                ],
                'learn_to_ride_q2' => [
                    'type'          => 'confidence',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q2'],
                    'sub_questions' => [
                        'learn_to_ride_q2a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q2a'],
                    ],
                ],
                'learn_to_ride_q3' => [
                    'type'          => 'likelihood',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q3'],
                    'sub_questions' => [
                        'learn_to_ride_q3a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q3a'],
                    ],
                ],
                'learn_to_ride_q4' => [
                    'type'          => 'how_much',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q4'],
                    'sub_questions' => [
                        'learn_to_ride_q4a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q4a'],
                    ],
                ],
                'learn_to_ride_q5' => [
                    'type'     => 'encouragement_list',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q5'],
                ],
                'learn_to_ride_q6' => [
                    'type'     => 'activity_level',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['learn_to_ride_q6'],
                ],
            ],
        ],

        'level_1' => [
            'course'    => 'Level 1',
            'questions' => [
                'level_1_q1' => [
                    'type'          => 'frequency',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q1'],
                    'sub_questions' => [
                        'level_1_q1a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q1a'],
                        'level_1_q1b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q1b'],
                        'level_1_q1c' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q1c'],
                    ],
                ],
                'level_1_q2' => [
                    'type'          => 'confidence',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q2'],
                    'sub_questions' => [
                        'level_1_q2a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q2a'],
                        'level_1_q2b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q2b'],
                    ],
                ],
                'level_1_q3' => [
                    'type'          => 'likelihood',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q3'],
                    'sub_questions' => [
                        'level_1_q3a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q3a'],
                    ],
                ],
                'level_1_q4' => [
                    'type'     => 'how_much',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q4'],
                ],
                'level_1_q5' => [
                    'type'     => 'encouragement_list',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q5'],
                ],
                'level_1_q6' => [
                    'type'     => 'activity_level',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_q6'],
                ],
            ],
        ],

        'level_2' => [
            'course'    => 'Level 2',
            'questions' => [
                'level_2_q1' => [
                    'type'          => 'frequency',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q1'],
                    'sub_questions' => [
                        'level_2_q1a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q1a'],
                        'level_2_q1b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q1b'],
                        'level_2_q1c' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q1c'],
                    ],
                ],
                'level_2_q2' => [
                    'type'          => 'confidence',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q2'],
                    'sub_questions' => [
                        'level_2_q2a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q2a'],
                        'level_2_q2b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q2b'],
                    ],
                ],
                'level_2_q3' => [
                    'type'          => 'likelihood',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q3'],
                    'sub_questions' => [
                        'level_2_q3a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q3a'],
                        'level_2_q3b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q3b'],
                    ],
                ],
                'level_2_q4' => [
                    'type'     => 'safety',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q4'],
                ],
                'level_2_q5' => [
                    'type'     => 'how_much',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q5'],
                ],
                'level_2_q6' => [
                    'type'     => 'encouragement_list',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q6'],
                ],
                'level_2_q7' => [
                    'type'     => 'change_confidence',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q7'],
                ],
                'level_2_q8' => [
                    'type'     => 'activity_level',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q8'],
                ],
                'level_2_q9' => [
                    'type'     => 'virtual_rider_training',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q9'],
                ],
                'level_2_q10' => [
                    'type'     => 'feeling_about_cycling_on_road',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q10'],
                ],
                'level_2_q11' => [
                    'type'     => 'new_cycling_skills',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q11'],
                ],
                'level_2_q12' => [
                    'type'     => 'new_cycling_skills_likelihood',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q12'],
                ],
                'level_2_q13' => [
                    'type'     => 'new_cycling_skills_likelihood_parent',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_2_q13'],
                ],
            ],
        ],
        'level_1_2' => [
            'course'    => 'Level 1 & 2',
            'questions' => [
                'level_1_2_q1' => [
                    'type'          => 'frequency',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q1'],
                    'sub_questions' => [
                        'level_1_2_q1a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q1a'],
                        'level_1_2_q1b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q1b'],
                        'level_1_2_q1c' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q1c'],
                    ],
                ],
                'level_1_2_q2' => [
                    'type'          => 'confidence',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q2'],
                    'sub_questions' => [
                        'level_1_2_q2a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q2a'],
                        'level_1_2_q2b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q2b'],
                    ],
                ],
                'level_1_2_q3' => [
                    'type'          => 'likelihood',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q3'],
                    'sub_questions' => [
                        'level_1_2_q3a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q3a'],
                        'level_1_2_q3b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q3b'],
                    ],
                ],
                'level_1_2_q4' => [
                    'type'     => 'safety',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q4'],
                ],
                'level_1_2_q5' => [
                    'type'     => 'how_much',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q5'],
                ],
                'level_1_2_q6' => [
                    'type'     => 'encouragement_list',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q6'],
                ],
                'level_1_2_q7' => [
                    'type'     => 'change_confidence',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q7'],
                ],
                'level_1_2_q8' => [
                    'type'     => 'activity_level',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_1_2_q8'],
                ],
            ],
        ],

        'level_3' => [
            'course'    => 'Level 3',
            'questions' => [
                'level_3_q1' => [
                    'type'          => 'frequency',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q1'],
                    'sub_questions' => [
                        'level_3_q1a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q1a'],
                        'level_3_q1b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q1b'],
                        'level_3_q1c' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q1c'],
                    ],
                ],
                'level_3_q2' => [
                    'type'          => 'confidence',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q2'],
                    'sub_questions' => [
                        'level_3_q2a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q2a'],
                        'level_3_q2b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q2b'],
                    ],
                ],
                'level_3_q3' => [
                    'type'          => 'likelihood',
                    'question'      => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q3'],
                    'sub_questions' => [
                        'level_3_q3a' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q3a'],
                        'level_3_q3b' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q3b'],
                    ],
                ],
                'level_3_q4' => [
                    'type'     => 'safety',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q4'],
                ],
                'level_3_q5' => [
                    'type'     => 'how_much',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q5'],
                ],
                'level_3_q6' => [
                    'type'     => 'encouragement_list',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q6'],
                ],
                'level_3_q7' => [
                    'type'     => 'change_confidence',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q7'],
                ],
                'level_3_q8' => [
                    'type'     => 'activity_level',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q8'],
                ],
                'level_3_q9' => [
                    'type'     => 'virtual_rider_training',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q9'],
                ],
                'level_3_q10' => [
                    'type'     => 'feeling_about_cycling_on_road',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q10'],
                ],
                'level_3_q11' => [
                    'type'     => 'new_cycling_skills',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q11'],
                ],
                'level_3_q12' => [
                    'type'     => 'new_cycling_skills_likelihood',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q12'],
                ],
                'level_3_q13' => [
                    'type'     => 'new_cycling_skills_likelihood_parent',
                    'question' => self::PARENT_FOLLOW_UP_SURVEY_QUESTIONS_AND_OPTIONS['level_3_q13'],
                ],
            ],
        ],
    ];

    public static function getMainHeadings()
    {

        $mainHeadings = [];
        foreach (self::PARENT_FOLLOW_UP_SURVEY_HEADINGS as $index => $heading) {

            // Fixed metadata columns – keep first row empty so labels appear only in second row
            if (is_string($heading)) {
                $mainHeadings[] = '';
                continue;
            }

            // Course-specific sections
            if (is_array($heading) && isset($heading['course'], $heading['questions'])) {
                $courseLabel            = $heading['course'];
                $courseFirstColumnAdded = false;

                foreach ($heading['questions'] as $questionConfig) {
                    // Determine how many columns this question spans
                    if (isset($questionConfig['sub_questions']) && is_array($questionConfig['sub_questions'])) {
                        $columnCount = count($questionConfig['sub_questions']);
                    } else {
                        $columnCount = 1;
                    }

                    // For the very first column within this course, print the course name;
                    // for all subsequent columns in the same course block, use empty strings.
                    for ($i = 0; $i < $columnCount; $i++) {
                        if (! $courseFirstColumnAdded) {
                            $mainHeadings[]        = $courseLabel;
                            $courseFirstColumnAdded = true;
                        } else {
                            $mainHeadings[] = '';
                        }
                    }
                }
            }
        }

        return $mainHeadings;
    }

    /**
     * Second heading row:
     * - empty for fixed metadata columns
     * - question text (e.g. "balance_q1:Q.1 ...") for the first column of each question,
     *   then blanks for any additional sub-question columns for that question.
     */
    public static function getQuestionHeadings()
    {
        $questionHeadings = [];

        foreach (self::PARENT_FOLLOW_UP_SURVEY_HEADINGS as $index => $heading) {
            // Metadata columns: keep this row empty
            if (is_string($heading)) {
                $questionHeadings[] = '';
                continue;
            }

            if (is_array($heading) && isset($heading['questions'])) {
                foreach ($heading['questions'] as $questionCode => $questionConfig) {
                    // If the question has sub-questions, show the main question text in this row
                    // above the group of sub-question columns.
                    if (isset($questionConfig['sub_questions']) && is_array($questionConfig['sub_questions'])) {
                        $columnCount = count($questionConfig['sub_questions']);

                        for ($i = 0; $i < $columnCount; $i++) {
                            if ($i === 0) {
                                $questionHeadings[] = $questionConfig['question'];
                            } else {
                                $questionHeadings[] = '';
                            }
                        }
                        continue;
                    }

                    // If there are no sub-questions, this row should be empty for that column;
                    // the actual question text will only appear in the third heading row.
                    $questionHeadings[] = '';
                }
            }
        }

        return $questionHeadings;
    }

    public static function getHeadings() {

        $headings = [];
        foreach(self::PARENT_FOLLOW_UP_SURVEY_HEADINGS as $index => $heading){
            // For fixed metadata, use the label directly
            if(is_string($heading)){
                $headings[] = $heading;
                continue;
            }

            // For course-specific sections, walk through questions then any sub-questions
            if (is_array($heading) && isset($heading['questions'])) {
                foreach ($heading['questions'] as $questionCode => $questionConfig) {
                    if (isset($questionConfig['sub_questions']) && is_array($questionConfig['sub_questions'])) {
                        foreach ($questionConfig['sub_questions'] as $subIndex => $subHeading) {
                            $headings[] = $subHeading;
                        }
                    } else {
                        $headings[] = $questionConfig['question'];
                    }
                }
            }
        }

        return $headings;
    }


}
