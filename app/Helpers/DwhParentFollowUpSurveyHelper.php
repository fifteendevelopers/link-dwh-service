<?php

namespace App\Helpers;

class DwhParentFollowUpSurveyHelper
{
    public static function getHeadings(): array
    {
        return [
            'Grant Number',
            'Grant Source',
            'Grant Recipient',
            'Delivery ID',
            'Rider ID',
            'Training Provider',
            'School/Organisation',
            'School URN',
            'Course Level',
            'Received Date',
            'Invitation Month',
            'Q1a - Freq School',
            'Q1b - Freq Leisure',
            'Q1c - Freq Exercise',
            'Q2a - Conf Use Cycle',
            'Q2b - Conf Cycle Roads',
            'Q3a - Enc Use Cycle',
            'Q3b - Enc Cycle Roads',
            'Q4 - Safety Roads',
            'Q5 - Child Desire',
            'Q6 - Encouragement Factors',
            'Q7 - Conf Change',
            'Q8 - Physical Activity'
        ];
    }

    // Comprehensive label glossary to map raw question codes (o1, o2) back to human-readable strings
    public const OPTION_LABELS = [
        'q1a_freq_school' => ['o1' => 'Never', 'o2' => 'Less than once a week', 'o3' => '1-2 days a week', 'o4' => '3-4 days a week', 'o5' => 'Every day'],
        'q1b_freq_leisure' => ['o1' => 'Never', 'o2' => 'Less than once a week', 'o3' => '1-2 days a week', 'o4' => '3-4 days a week', 'o5' => 'Every day'],
        'q1c_freq_exercise' => ['o1' => 'Never', 'o2' => 'Less than once a week', 'o3' => '1-2 days a week', 'o4' => '3-4 days a week', 'o5' => 'Every day'],
        'q2a_conf_use_cycle' => ['o1' => 'Not at all confident', 'o2' => 'Not very confident', 'o3' => 'Neutral', 'o4' => 'Somewhat confident', 'o5' => 'Very confident'],
        'q2b_conf_cycle_roads' => ['o1' => 'Not at all confident', 'o2' => 'Not very confident', 'o3' => 'Neutral', 'o4' => 'Somewhat confident', 'o5' => 'Very confident'],
        'q3a_enc_use_cycle' => ['o1' => 'Strongly disagree', 'o2' => 'Disagree', 'o3' => 'Neutral', 'o4' => 'Agree', 'o5' => 'Strongly agree'],
        'q3b_enc_cycle_roads' => ['o1' => 'Strongly disagree', 'o2' => 'Disagree', 'o3' => 'Neutral', 'o4' => 'Agree', 'o5' => 'Strongly agree'],
        'q4_safety_roads' => ['o1' => 'Very unsafe', 'o2' => 'Unsafe', 'o3' => 'Neutral', 'o4' => 'Safe', 'o5' => 'Very safe'],
        'q5_child_desire' => ['o1' => 'A lot less', 'o2' => 'A bit less', 'o3' => 'About the same', 'o4' => 'A bit more', 'o5' => 'A lot more'],
        'q7_conf_change' => ['o1' => 'Decreased a lot', 'o2' => 'Decreased a bit', 'o3' => 'Stayed the same', 'o4' => 'Increased a bit', 'o5' => 'Increased a lot'],
        'q8_physical_activity' => ['o1' => 'Much less active', 'o2' => 'A bit less active', 'o3' => 'About the same', 'o4' => 'A bit more active', 'o5' => 'Much more active'],

        // Multi-select option lookup keys for Q6
        'enc_factors' => [
            'o1'  => 'More direct cycle routes, ideally with dedicated cycle lanes/tracks',
            'o2'  => 'Presence and awareness of local cycle routes',
            'o3'  => 'Secure storage at school or home',
            'o4'  => 'Smoother road surfaces for cycling',
            'o5'  => 'Me, the parent/carer, having greater confidence using a cycle',
            'o6'  => 'Cycle maintenance or repair courses',
            'o7'  => 'Local initiatives that facilitate cycling (for example, group rides)',
            'o8'  => 'Ability to purchase a cycle',
            'o9'  => 'My child does not want to cycle more',
            'o10' => 'None of the above'
        ]
    ];

    public static function getLabel(string $question, ?string $option): string
    {
        if (empty($option)) return '';
        return self::OPTION_LABELS[$question][$option] ?? $option;
    }

    public static function getMultiSelectLabels(?string $json): string
    {
        if (empty($json)) return '';
        $codes = json_decode($json, true);
        if (!is_array($codes)) return '';

        $labels = [];
        foreach ($codes as $code) {
            // Extracts the base option key (e.g. 'o1' out of 'level_1_q5_o1' or 'o1')
            if (preg_match('/_o(\d+)$/', $code, $matches)) {
                $optionKey = 'o' . $matches[1];
            } else {
                $optionKey = $code;
            }
            $labels[] = self::OPTION_LABELS['enc_factors'][$optionKey] ?? $optionKey;
        }

        return implode('; ', $labels);
    }
}
